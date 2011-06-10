-module (mc_connection).

-export([loop/2]).
-export([respond/5, respond/4]).

-include("mc_constants.hrl").

bin_size(undefined) -> 0;
bin_size(IoList) -> iolist_size(IoList).

xmit(_Socket, undefined) -> ok;
xmit(Socket, List) when is_list(List) -> xmit(Socket, list_to_binary(List));
xmit(Socket, Data) -> gen_tcp:send(Socket, Data).

respond(Magic, Socket, OpCode, Opaque, Res) ->
    KeyLen = bin_size(Res#mc_response.key),
    ExtraLen = bin_size(Res#mc_response.extra),
    BodyLen = bin_size(Res#mc_response.body) + (KeyLen + ExtraLen),
    Status = Res#mc_response.status,
    CAS = Res#mc_response.cas,
    ok = gen_tcp:send(Socket, <<Magic, OpCode:8, KeyLen:16,
                               ExtraLen:8, 0:8, Status:16,
                               BodyLen:32, Opaque:32, CAS:64>>),
    ok = xmit(Socket, Res#mc_response.extra),
    ok = xmit(Socket, Res#mc_response.key),
    ok = xmit(Socket, Res#mc_response.body).

respond(Socket, OpCode, Opaque, Res) ->
    respond(?RES_MAGIC, Socket, OpCode, Opaque, Res).

% Read-data special cases a 0 size to just return an empty binary.
read_data(_Socket, 0, _ForWhat) -> <<>>;
read_data(Socket, N, ForWhat) ->
    error_logger:info_msg("Reading ~p bytes of ~p~n", [N, ForWhat]),
    {ok, Data} = gen_tcp:recv(Socket, N),
    Data.

read_message(Socket, KeyLen, ExtraLen, BodyLen) ->
    Extra = read_data(Socket, ExtraLen, extra),
    Key = read_data(Socket, KeyLen, key),
    Body = read_data(Socket, BodyLen - (KeyLen + ExtraLen), body),

    {Extra, Key, Body}.

process_message(Socket, StorageServer, {ok, <<?REQ_MAGIC:8, ?TAP_CONNECT:8, KeyLen:16,
                                            ExtraLen:8, 0:8, _VBucket:16,
                                            BodyLen:32,
                                            Opaque:32,
                                            CAS:64>>}) ->
    error_logger:info_msg("Got a tap connection request for ~p.~n", [StorageServer]),

    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),

    % Hand the request off to the server.
    gen_server:cast(StorageServer, {?TAP_CONNECT, Extra, Key, Body, CAS, Socket, Opaque});
process_message(Socket, StorageServer, {ok, <<?REQ_MAGIC:8, ?STAT:8, KeyLen:16,
                                            ExtraLen:8, 0:8, _VBucket:16,
                                            BodyLen:32,
                                            Opaque:32,
                                            CAS:64>>}) ->
    error_logger:info_msg("Got a stat request for ~p.~n", [StorageServer]),

    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),

    % Hand the request off to the server.
    gen_server:cast(StorageServer, {?STAT, Extra, Key, Body, CAS, Socket, Opaque});
process_message(Socket, StorageServer, {ok, <<?REQ_MAGIC:8, OpCode:8, KeyLen:16,
                                            ExtraLen:8, 0:8, VBucket:16,
                                            BodyLen:32,
                                            Opaque:32,
                                            CAS:64>>}) ->
    error_logger:info_msg("Got message of type ~p to give to ~p.~n",
                          [OpCode, StorageServer]),

    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),

    % Hand the request off to the server.
    Res = gen_server:call(StorageServer, {OpCode, VBucket, Extra, Key, Body, CAS}),

    respond(Socket, OpCode, Opaque, Res).

loop(Socket, Handler) ->
    process_message(Socket, Handler, gen_tcp:recv(Socket, ?HEADER_LEN)),
    loop(Socket, Handler).
