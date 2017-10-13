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
  cJSON *json;
  cJSON *sort;
  RedisModuleString *reply;
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

void *DoSearch(void *arg) {
  CommandCtx *cctx = arg;
  RedisModuleBlockedClient *bc = cctx->bc;
  RedisModuleString **argv = cctx->argv;
  int argc = cctx->argc;
  RedisModule_Free(cctx);

  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

  // open the key and make sure it's indeed a HASH and not empty
  /*
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH &&
      RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    RM_CloseKey(key);
    RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    return NULL;
  }*/

  // get the current value of the hash element
  RedisModule_ThreadSafeContextLock(ctx);
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "HVALS", "s", argv[1]);
  RedisModule_ThreadSafeContextUnlock(ctx);

  if (reply == NULL) {
    RedisModule_ReplyWithError(ctx, "ERR reply is NULL");
    goto free_argv;
  } else if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
    RedisModule_ReplyWithCallReply(ctx, reply);
    goto free_reply;
  }

  size_t ct_reply = RedisModule_CallReplyLength(reply);

  const char *filter = RedisModule_StringPtrLen(argv[2], NULL);
  const char *sortName = RedisModule_StringPtrLen(argv[3], NULL);
  Vector *res = NewVector(Entity *, 200);

  Entity *ext;
  cJSON *json_root;
  cJSON *json_value;
  RedisModuleCallReply *cr_ext;
  RedisModuleString *json_body;
  int found;
  char *fields[] = {"name", "department", "pin"};
  for (int i = 0; i < ct_reply; i++) {
    cr_ext = RedisModule_CallReplyArrayElement(reply, i);
    json_body = RedisModule_CreateStringFromCallReply(cr_ext);
    json_root = cJSON_Parse(RedisModule_StringPtrLen(json_body, NULL));

    found = 0;
    for (int j = 0; j < 3; j++) {
      json_value = cJSON_GetObjectItem(json_root, fields[j]);
      // strstr case sensitive
      if (strcasestr(json_value->valuestring, filter)) {
        found = 1;
        break;
      }
    }
    if (found == 1) {
      ext = RedisModule_Alloc(sizeof(Entity));
      ext->json = json_root;
      ext->sort = cJSON_GetObjectItem(json_root, sortName);
      ext->reply = json_body;
      Vector_Push(res, ext);
      ext = NULL;
    } else {
      RedisModule_FreeString(ctx, json_body);
      cJSON_Delete(json_root);
    }
  }

  long long tmp;
  RedisModule_StringToLongLong(argv[4], &tmp);
  size_t page_start = (size_t)tmp;
  RedisModule_StringToLongLong(argv[5], &tmp);
  size_t page_end = (size_t)tmp;

  if (Vector_Size(res) > page_start) {
    int sort = 1;
    Vector_Sort(res, &sort, compare);
  }

  if (Vector_Size(res) > 0) {
    Entity *ext2;
    RedisModule_ReplyWithArray(ctx, min(Vector_Size(res), page_end) - page_start + 1);
    RedisModule_ReplyWithDouble(ctx, Vector_Size(res));
    for (size_t idx = 0; idx < Vector_Size(res); idx++) {
      Vector_Get(res, idx, &ext2);
      cJSON_Delete(ext2->json);
      if (idx >= page_start && idx < page_end) {
        RedisModule_ReplyWithString(ctx, ext2->reply);
      }
      RedisModule_FreeString(ctx, ext2->reply);
      RedisModule_Free(ext2);
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
  if (argc < 6 || argc % 2 != 0) {
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
