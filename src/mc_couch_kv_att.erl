-module(mc_couch_kv_att).

-include("couch_db.hrl").

-export([get/2, set/5, delete/2]).

-spec set(_, binary(), integer(), integer(), binary()) -> integer().
set(Db, Key, Flags, Expiration, Value) ->
    ToStore = [{<<"_id">>, Key},
               {<<"$flags">>, Flags},
               {<<"$expiration">>, Expiration},
               {<<"_attachments">>,
                {[{<<"value">>,
                  {[{<<"content_type">>, <<"application/content-stream">>},
                   {<<"data">>, base64:encode(Value)}]}}]}}],

    WithRev = mc_couch_util:addRev(Db, Key, ToStore),

    Encoded = iolist_to_binary(couch_util:json_encode(
                                 {WithRev})),
    ?LOG_INFO("Doc I'm writing ~p.", [Encoded]),

    Doc = couch_doc:from_json_obj({WithRev}),

    couch_db:update_doc(Db, Doc, []),
    0.

dig_out_attachment(Doc, FileName) ->
    case [A || A <- Doc#doc.atts, A#att.name == FileName] of
        [] ->
            not_found;
        [Att] ->
            Segs = couch_doc:att_foldl(Att, fun(Seg, Acc) -> [Seg|Acc] end, []),
            {ok, iolist_to_binary(lists:reverse(Segs))}
    end.

%% ok, Flags, Cas, Data
-spec get(_, binary()) -> {ok, integer(), integer(), binary()}.
get(Db, Key) ->
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            Flags = proplists:get_value(<<"$flags">>, EJson, 0),
            case dig_out_attachment(Doc, <<"value">>) of
                {ok, AttData} -> {ok, Flags, 0, AttData};
                _ -> not_found
            end;
        _ -> not_found
    end.

delete(Db, Key) -> mc_couch_util:delete(Db, Key).
