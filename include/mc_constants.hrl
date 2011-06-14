-define(HEADER_LEN, 24).
-define(REQ_MAGIC, 16#80).
-define(RES_MAGIC, 16#81).

% Command codes.
-define(GET,         16#00).
-define(SET,         16#01).
-define(ADD,         16#02).
-define(REPLACE,     16#03).
-define(DELETE,      16#04).
-define(INCREMENT,   16#05).
-define(DECREMENT,   16#06).
-define(QUIT,        16#07).
-define(FLUSH,       16#08).
-define(GETQ,        16#09).
-define(NOOP,        16#0a).
-define(VERSION,     16#0b).
-define(GETK,        16#0c).
-define(GETKQ,       16#0d).
-define(APPEND,      16#0e).
-define(PREPEND,     16#0f).
-define(STAT,        16#10).
-define(SETQ,        16#11).
-define(ADDQ,        16#12).
-define(REPLACEQ,    16#13).
-define(DELETEQ,     16#14).
-define(INCREMENTQ,  16#15).
-define(DECREMENTQ,  16#16).
-define(QUITQ,       16#17).
-define(FLUSHQ,      16#18).
-define(APPENDQ,     16#19).
-define(PREPENDQ,    16#1a).
-define(RGET,        16#30).
-define(RSET,        16#31).
-define(RSETQ,       16#32).
-define(RAPPEND,     16#33).
-define(RAPPENDQ,    16#34).
-define(RPREPEND,    16#35).
-define(RPREPENDQ,   16#36).
-define(RDELETE,     16#37).
-define(RDELETEQ,    16#38).
-define(RINCR,       16#39).
-define(RINCRQ,      16#3a).
-define(RDECR,       16#3b).
-define(RDECRQ,      16#3c).

-define(CREATE_BUCKET, 16#85).
-define(DELETE_BUCKET, 16#86).
-define(SELECT_BUCKET, 16#89).

%% vbucket stuff
-define(SET_VBUCKET_STATE, 16#3d).
-define(GET_VBUCKET_STATE, 16#3e).
-define(DELETE_VBUCKET,    16#3f).

%% VBucket States

-define(VB_STATE_ACTIVE,  16#1).
-define(VB_STATE_REPLICA, 16#2).
-define(VB_STATE_PENDING, 16#3).
-define(VB_STATE_DEAD,    16#4).

%% TAP

-define(TAP_CONNECT,           16#40).
-define(TAP_MUTATION,          16#41).
-define(TAP_DELETE,            16#42).
-define(TAP_FLUSH,             16#43).
-define(TAP_OPAQUE,            16#44).
-define(TAP_VBUCKET_SET,       16#45).
-define(TAP_CHECKPOINT_START,  16#46).
-define(TAP_CHECKPOINT_END,    16#47).


% Response status codes.
-define(SUCCESS,          16#00).
-define(KEY_ENOENT,       16#01).
-define(KEY_EEXISTS,      16#02).
-define(E2BIG,            16#03).
-define(EINVAL,           16#04).
-define(NOT_STORED,       16#05).
-define(DELTA_BADVAL,     16#06).
-define(UNKNOWN_COMMAND,  16#81).
-define(ENOMEM,           16#82).
-define(EINTERNAL,        16#84).

-record(mc_response, {
          status=0,
          extra,
          key,
          body,
          cas=0
         }).
