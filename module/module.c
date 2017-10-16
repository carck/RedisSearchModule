#define REDISMODULE_EXPERIMENTAL_API
#include <stdio.h>
#include <stdlib.h>
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/vector.h"
#include "../rmutil/strings.h"
#include "../rmutil/cJSON.h"
#include "../rmutil/thread_pool.h"

#define min(a, b) (((a) < (b)) ? (a) : (b))

typedef struct {
  RedisModuleBlockedClient *bc;
  RedisModuleString **argv;
  int argc;
} CommandCtx;

typedef struct {
  RedisModuleString *key;
  const char *query;
  int len_query;
  const char *filters[10];
  int ct_filter;
  int page_start;
  int page_end;
  const char *sortName;
  int sortDirection;
} SearchForm;

typedef struct {
  cJSON *doc;
  cJSON *sort;
  RedisModuleString *rawString;
} Entity;

int compare(void *arg, const void *a, const void *b) {
  Entity *ext1 = *(Entity **)a;
  Entity *ext2 = *((Entity **)b);
  int sort = *(int *)arg;
  cJSON *pin1 = ext1->sort;
  cJSON *pin2 = ext2->sort;
  return sort * strcmp(pin1->valuestring, pin2->valuestring);
}

void FreeArgv(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  for (int i = 0; i < argc; i++) {
    RedisModule_FreeString(ctx, argv[i]);
  }
  RedisModule_Free(argv);
}

void InitSearchFrom(SearchForm *form, RedisModuleString **argv, int argc) {
  form->key = argv[1];
  form->query = RedisModule_StringPtrLen(argv[2], NULL);
  form->len_query = strlen(form->query);
  const char *sortName = RedisModule_StringPtrLen(argv[3], NULL);
  form->sortDirection = (*sortName == '-' ? 1 : -1);
  form->sortName = sortName + 1;
  long long tmp;
  RedisModule_StringToLongLong(argv[4], &tmp);
  form->page_start = (size_t)tmp;
  RedisModule_StringToLongLong(argv[5], &tmp);
  form->page_end = (size_t)tmp;
  form->ct_filter = argc - 6;
  if (form->ct_filter > 0) {
    for (int i = 0; i < form->ct_filter; i++) {
      form->filters[i] = RedisModule_StringPtrLen(argv[6 + i], NULL);
    }
  }
}

int IsMatch(cJSON *doc, SearchForm *form) {
  cJSON *json_value;
  const static char *fields[] = {"name", "department", "pin", "number"};
  if (form->ct_filter > 0) {
    for (int i = 0; i < form->ct_filter; i += 2) {
      json_value = cJSON_GetObjectItem(doc, form->filters[i]);
      if (strcmp(json_value->valuestring, form->filters[i + 1]) != 0) {
        return 0;
      }
    }
  }
  if(form->len_query == 0)
    return 1;
  for (int j = 0; j < 4; j++) {
    json_value = cJSON_GetObjectItem(doc, fields[j]);
    // strstr case sensitive
    if (strcasestr(json_value->valuestring, form->query)) {
      return 1;
    }
  }
  return 0;
}

void *DoSearch(void *arg) {
  CommandCtx *cctx = arg;

  SearchForm form;
  InitSearchFrom(&form, cctx->argv, cctx->argc);
  RedisModuleBlockedClient *bc = cctx->bc;
  RedisModuleString **argv = cctx->argv;
  int argc = cctx->argc;
  RedisModule_Free(cctx);

  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

  // get element to search
  RedisModule_ThreadSafeContextLock(ctx);
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "HVALS", "s", form.key);
  RedisModule_ThreadSafeContextUnlock(ctx);

  if (reply == NULL) {
    RedisModule_ReplyWithError(ctx, "ERR reply is NULL");
    goto free_argv;
  } else if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
    RedisModule_ReplyWithCallReply(ctx, reply);
    goto free_reply;
  }

  size_t ct_reply = RedisModule_CallReplyLength(reply);
  Vector *res = NewVector(Entity *, min(200, ct_reply));

  Entity *ext;
  cJSON *doc;
  RedisModuleCallReply *cr_ext;
  RedisModuleString *json_body;
  for (int i = 0; i < ct_reply; i++) {
    cr_ext = RedisModule_CallReplyArrayElement(reply, i);
    json_body = RedisModule_CreateStringFromCallReply(cr_ext);
    doc = cJSON_Parse(RedisModule_StringPtrLen(json_body, NULL));

    if (IsMatch(doc, &form) == 1) {
      ext = RedisModule_Alloc(sizeof(Entity));
      ext->doc = doc;
      ext->sort = cJSON_GetObjectItem(doc, form.sortName);
      ext->rawString = json_body;
      Vector_Push(res, ext);
      ext = NULL;
    } else {
      RedisModule_FreeString(ctx, json_body);
      cJSON_Delete(doc);
    }
  }

  if (Vector_Size(res) > form.page_start) {
    Vector_Sort(res, &form.sortDirection, compare);
  }

  if (Vector_Size(res) > 0) {
    RedisModule_ReplyWithArray(ctx, min(Vector_Size(res), form.page_end) - form.page_start + 1);
    RedisModule_ReplyWithDouble(ctx, Vector_Size(res));
    for (size_t idx = 0; idx < Vector_Size(res); idx++) {
      Vector_Get(res, idx, &ext);
      cJSON_Delete(ext->doc);
      if (idx >= form.page_start && idx < form.page_end) {
        RedisModule_ReplyWithString(ctx, ext->rawString);
      }
      RedisModule_FreeString(ctx, ext->rawString);
      RedisModule_Free(ext);
    }
  } else {
    RedisModule_ReplyWithNull(ctx);
  }

  Vector_Free(res);
free_reply:
  RedisModule_FreeCallReply(reply);
free_argv:
  FreeArgv(ctx, argv, argc);
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_UnblockClient(bc, NULL);
  return NULL;
}
/*
* nr.search <key> <text> <sort> [<filter> <value>]
* Custom search search for hash set
*/
int HSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // check arguments
  if (argc < 6 || argc % 2 != 0 || argc > 16) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);

  // copy argv to use in thread
  RedisModuleString **argvSafe = RedisModule_Alloc(sizeof(RedisModuleString *) * argc);
  for (int i = 0; i < argc; i++) {
    argvSafe[i] = RedisModule_CreateStringFromString(ctx, argv[i]);
  }

  CommandCtx *cctx = RedisModule_Alloc(sizeof(CommandCtx));
  cctx->bc = bc;
  cctx->argv = argvSafe;
  cctx->argc = argc;

  if (tpool_add_work(DoSearch, (void *)cctx) != 0) {
    RedisModule_AbortBlock(bc);
    RedisModule_ReplyWithError(ctx, "Sorry can't create a thread");
  }

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int poolSize = 6;
  if (tpool_create((int)poolSize) != 0) {
    return REDISMODULE_ERR;
  }
  // Register the module itself
  if (RedisModule_Init(ctx, "nr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // register NR.Search - using the shortened utility registration macro
  RMUtil_RegisterWriteCmd(ctx, "nr.search", HSearchCommand);

  return REDISMODULE_OK;
}
