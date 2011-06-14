-module(mc_tap).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-define(DUMP_ONLY, 2).
-define(DUMP_AND_LIST_VBUCKETS, 6).

-export([run/5]).

%% We're pretty specific about the type of tap connections we can handle.
run(State, Opaque, Socket, <<?DUMP_ONLY:32>>, <<>>) ->
    spawn_link(fun() ->
                       DbName = mc_daemon:db_prefix(State),
                       lists:foreach(fun({VB,_VBState}) ->
                                             process_tap_stream(DbName, Opaque, VB, Socket)
                                     end,
                                     mc_couch_vbucket:list_vbuckets(State)),
                        terminate_tap_stream(Socket, Opaque, 0)
               end);
run(State, Opaque, Socket,
    <<?DUMP_AND_LIST_VBUCKETS:32>>, <<1:16, VBucketId:16>>) ->
    spawn_link(fun() -> process_tap_stream(mc_daemon:db_prefix(State), Opaque,
                                           VBucketId, Socket),
                        terminate_tap_stream(Socket, Opaque, VBucketId)
               end);
run(State, Opaque, Socket, <<Flags:32>>, Extra) ->
    ?LOG_INFO("MC tap: invalid request: ~p/~p/~p/~p/~p",
              [State, Opaque, Socket, Flags, Extra]),
    mc_connection:respond(Socket, ?TAP_CONNECT, Opaque,
                          #mc_response{status=?EINVAL,
                                       body="Only dump+1 vbucket is allowed"}).

emit_tap_doc(Socket, Opaque, VBucketId, Key, Flags, Expiration, _Cas, Data) ->
    Extras = <<0:16, 0:16,    %% length, flags
               0:8,           %% TTL
               0:8, 0:8, 0:8, %% reserved
               Flags:32, Expiration:32>>,
    mc_connection:respond(?REQ_MAGIC, Socket, ?TAP_MUTATION, Opaque,
                          #mc_response{key=Key, status=VBucketId,
                                       extra=Extras, body=Data}).


process_tap_stream(BaseDbName, Opaque, VBucketId, Socket) ->
    ?LOG_INFO("MC tap: processing: ~p/~p/~p/~p", [BaseDbName, Opaque, Socket, VBucketId]),

    DbName = lists:flatten(io_lib:format("~s/~p", [BaseDbName, VBucketId])),
    {ok, Db} = couch_db:open(list_to_binary(DbName), []),

    AdapterFun = fun(#full_doc_info{id=Id}=FullDocInfo, _Offset, Acc) ->
                         case couch_doc:to_doc_info(FullDocInfo) of
                             #doc_info{revs=[#rev_info{deleted=false}|_]} = _DocInfo ->
                                 {ok, Flags, Expiration, Cas, Data} = mc_couch_kv:get(Db, Id),
                                 emit_tap_doc(Socket, Opaque, VBucketId, Id,
                                              Flags, Expiration, Cas, Data),
                                 {ok, Acc};
                             #doc_info{revs=[#rev_info{deleted=true}|_]} ->
                                 {ok, Acc}
                         end
                 end,
    {ok, _LastOffset, _FoldResult} = couch_db:enum_docs(Db,
                                                        AdapterFun, fold_thing,
                                                        []),

    couch_db:close(Db).

terminate_tap_stream(Socket, Opaque, Status) ->
    TerminalExtra = <<8:16, 0:16,      %% length, flags
                      0:8,             %% TTL
                      0:8, 0:8, 0:8>>, %% reserved
    mc_connection:respond(?REQ_MAGIC, Socket, ?TAP_OPAQUE, Opaque,
                          #mc_response{extra=TerminalExtra, status=Status}).
