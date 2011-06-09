-module(mc_couch_kv).

-include("couch_db.hrl").

-export([get/2, set/6, delete/2]).
-export([json_encode/1, json_decode/1]).

dig_out_attachment(Doc, FileName) ->
    case [A || A <- Doc#doc.atts, A#att.name == FileName] of
        [] ->
            not_found;
        [Att] ->
            Segs = couch_doc:att_foldl(Att, fun(Seg, Acc) -> [Seg|Acc] end, []),
            {ok, iolist_to_binary(lists:reverse(Segs))}
    end.

cleanup(EJson, []) -> EJson;
cleanup(EJson, [Hd|Tl]) -> cleanup(proplists:delete(Hd, EJson), Tl).
cleanup(EJson) ->
    cleanup(EJson, [<<"_id">>, <<"_rev">>, <<"$flags">>, <<"$expiration">>]).

addRev(Db, Key, Doc) ->
    case couch_db:get_doc_info(Db, Key) of
        {ok, #doc_info{revs=[#rev_info{rev=Rev,seq=Seq}]}} ->
            Doc#doc{revs={Seq,[Rev]}};
        _ ->
            Doc
    end.

json_encode(V) ->
    Handler =
    fun({L}) when is_list(L) ->
        {struct,L};
    (Bad) ->
        exit({json_encode, {bad_term, Bad}})
    end,
    (mochijson2:encoder([{handler, Handler}]))(V).

json_decode(V) ->
    try (mochijson2:decoder([{object_hook, fun({struct,L}) -> {L} end}]))(V)
    catch
        _Type:_Error ->
            throw({invalid_json,V})
    end.

%% ok, Flags, Cas, Data
-spec get(_, binary()) -> {ok, integer(), integer(), binary()}.
get(Db, Key) ->
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            ?LOG_INFO("Doc keys ~p.", [proplists:get_keys(EJson)]),
            Flags = proplists:get_value(<<"$flags">>, EJson, 0),

            case dig_out_attachment(Doc, <<"value">>) of
                {ok, AttData} ->
                    {ok, Flags, 0, AttData};
                _ ->
                    Encoded = iolist_to_binary(json_encode(
                                                 {cleanup(EJson)})),
                    ?LOG_INFO("MC daemon: got doc ~p:  ~p.", [Key, Encoded]),
                    {ok, Flags, 0, Encoded}
            end;
        _ -> not_found
    end.

mk_att_doc(Key, Flags, Expiration, Value, Reason) ->
    #doc{id=Key,
         body = {[
                  {<<"$flags">>, Flags},
                  {<<"$expiration">>, Expiration},
                  {<<"$att_reason">>, Reason}
                 ]},
         atts = [#att{
                    name= <<"value">>,
                    type= <<"application/content-stream">>,
                    data= Value}
                ]}.

%% Reject docs that have keys starting with _ or $
validate([]) -> ok;
validate([{<<$_:8,_/binary>>,_Val}|_Tl]) -> throw(invalid_key);
validate([{<<$$:8,_/binary>>, _Val}|_Tl]) -> throw(invalid_key);
validate([_|Tl]) -> validate(Tl).

parse_json(Value) ->
    {J} = json_decode(Value),
    validate(J),
    J.

mk_json_doc(Key, Flags, Expiration, Value) ->
    case (catch parse_json(Value)) of
        {invalid_json, _} ->
            mk_att_doc(Key, Flags, Expiration, Value, <<"invalid_json">>);
        invalid_key ->
            mk_att_doc(Key, Flags, Expiration, Value, <<"invalid_key">>);
        EJson ->
            #doc{id=Key,
                body={[
                    {<<"$flags">>, Flags},
                    {<<"$expiration">>, Expiration}
                    | cleanup(EJson)]}
                }
    end.

mk_doc(Key, Flags, Expiration, Value, WantJson) ->
    case WantJson of
        true ->
            mk_json_doc(Key, Flags, Expiration, Value);
        _ ->
            mk_att_doc(Key, Flags, Expiration, Value, <<"non-JSON mode">>)
    end.

-spec set(_, binary(), integer(), integer(), binary(), boolean()) -> integer().
set(Db, Key, Flags, Expiration, Value, JsonMode) ->
    Doc = addRev(Db, Key, mk_doc(Key, Flags, Expiration, Value, JsonMode)),
    couch_db:update_doc(Db, Doc, []),
    0.

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
