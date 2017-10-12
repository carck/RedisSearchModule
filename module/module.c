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
  RedisModuleString *reply;
} Ext;

int compare(void *arg, const void *a, const void *b) {
  Ext *ext1 = *(Ext **)a;
  Ext *ext2 = *((Ext **)b);
  cJSON *pin1 = cJSON_GetObjectItem(ext1->json, arg);
  cJSON *pin2 = cJSON_GetObjectItem(ext2->json, arg);
  return strcmp(pin1->valuestring, pin2->valuestring);
}

void FreeArgv(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  for (int i = 0; i < argc; i++) {
    RedisModule_FreeString(ctx, argv[i]);
  }
  RedisModule_Free(argv);
}

void *do_search(void *arg) {
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
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "HVALS", "s", argv[1]);
  RedisModule_ThreadSafeContextUnlock(ctx);

  if (rep == NULL) {
    RedisModule_ReplyWithError(ctx, "ERR reply is NULL");
    FreeArgv(ctx, argv, argc);
    return NULL;
  } else if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR) {
    RedisModule_ReplyWithCallReply(ctx, rep);
    FreeArgv(ctx, argv, argc);
    return NULL;
  }

  size_t replyCount = RedisModule_CallReplyLength(rep);

  const char *filter = RedisModule_StringPtrLen(argv[2], NULL);
  Vector *res = NewVector(Ext *, 200);

  Ext *ext;
  cJSON *json_root;
  cJSON *json_value;
  RedisModuleCallReply *cursor;
  RedisModuleString *json_body;
  int found;
  char *fields[] = {"name", "department", "pin"};
  for (int i = 0; i < replyCount; i++) {
    cursor = RedisModule_CallReplyArrayElement(rep, i);
    json_body = RedisModule_CreateStringFromCallReply(cursor);
    json_root = cJSON_Parse(RedisModule_StringPtrLen(json_body, NULL));

    found = 0;
    for (int j = 0; j < 3; j++) {
      json_value = cJSON_GetObjectItem(json_root, fields[j]);
      //strstr case sensitive
      if (strcasestr(json_value->valuestring, filter)) {
        found = 1;
        break;
      }
    }
    if (found == 1) {
      ext = RedisModule_Alloc(sizeof(Ext));
      ext->json = json_root;
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
    const char *sortName = RedisModule_StringPtrLen(argv[3], NULL);
    Vector_Sort(res, sortName, compare);
  }

  if (Vector_Size(res) > 0) {
    Ext *ext2;
    RedisModule_ReplyWithArray(ctx, min(Vector_Size(res), page_end) - page_start);
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

  FreeArgv(ctx, argv, argc);
  Vector_Free(res);
  RedisModule_FreeCallReply(rep);
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

  //copy argv to use in thread
  RedisModuleString **argvSafe = RedisModule_Alloc(sizeof(RedisModuleString *) * argc);
  for (int i = 0; i < argc; i++) {
    argvSafe[i] = RedisModule_CreateStringFromString(ctx, argv[i]);
  }

  CommandCtx *cctx = RedisModule_Alloc(sizeof(CommandCtx));
  cctx->bc = bc;
  cctx->argv = argvSafe;
  cctx->argc = argc;

  if (tpool_add_work(do_search, (void *)cctx) != 0) {
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
