%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc).

-export([
        get/2,
        get/3,
        put/3,
        put/4,
        delete/2,
        delete/3,
        list_buckets/0,
        list_buckets/1,
        list_keys/1,
        list_keys/2
    ]).

-define(DEFAULT_BACKEND_INSTANCE, default).

% TODO: improve specs
-spec put(BackendName :: atom(),Table :: binary(),Key::term(),Value::term()) ->  ok | {ok, term()} | {error, term()}.

put(Table, Key, Value) ->
    put(?DEFAULT_BACKEND_INSTANCE, Table, Key, Value).
put(BackendName, Table, Key, Value) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:put(ProcessName, Table, Key, Value).

-spec get(BackendName :: atom(), Table :: binary(),Key::term()) -> {ok,term()} | {error,term()}.
get(Table, Key) ->
    get(?DEFAULT_BACKEND_INSTANCE, Table, Key).
get(BackendName, Table, Key) -> 
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:get(ProcessName, Table, Key).

-spec delete(BackendName :: atom(),Table ::binary(),Key::term()) -> ok | {error, term()}.
delete(Table, Key) ->
    delete(?DEFAULT_BACKEND_INSTANCE, Table, Key).
delete(BackendName, Table, Key) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:delete(ProcessName, Table, Key).

list_keys(Table) -> list_keys(?DEFAULT_BACKEND_INSTANCE, Table).
list_keys(BackendName, Table) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:list_keys(ProcessName, Table).

list_buckets() ->
    list_buckets(?DEFAULT_BACKEND_INSTANCE).
list_buckets(BackendName) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:list_buckets(ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

process_name(BackendName) ->
    kvdbc_cfg:backend_val(BackendName, process_name).

module_name(BackendName) ->
    kvdbc_cfg:backend_val(BackendName, callback_module).
