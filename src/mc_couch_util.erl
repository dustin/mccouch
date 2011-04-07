-module(mc_couch_util).

-export([cleanup/1, addRev/3, delete/2]).

cleanup(EJson, []) -> EJson;
cleanup(EJson, [Hd|Tl]) -> cleanup(proplists:delete(Hd, EJson), Tl).
cleanup(EJson) ->
    cleanup(EJson, [<<"_id">>, <<"_rev">>, <<"$flags">>, <<"$expiration">>]).

addRev(Db, Key, ToStore) ->
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            [{<<"_rev">>, proplists:get_value(<<"_rev">>, EJson)} | ToStore];
        _ ->
            ToStore
    end.

-spec delete(_, binary()) -> ok|not_found.
delete(Db, Key) ->
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            DelMe = [{<<"_deleted">>, true},
                     {<<"_id">>, Key},
                     {<<"_rev">>, proplists:get_value(<<"_rev">>, EJson)}],
            couch_db:update_doc(Db,
                                couch_doc:from_json_obj({DelMe}), []),
            ok;
        _ ->
            not_found
    end.
