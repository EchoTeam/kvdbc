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

-include_lib("riakc_cluster/include/riakc_cluster_types.hrl").

-define(DEFAULT_BACKEND_INSTANCE, default).

-spec put(Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(Table, Key, Value) ->
    put(?DEFAULT_BACKEND_INSTANCE, Table, Key, Value).

-spec put(BackendName :: atom(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(BackendName, Table, Key, Value) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:put(BackendName, ProcessName, Table, Key, Value).

-spec get(Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(Table, Key) ->
    get(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec get(BackendName :: atom(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(BackendName, Table, Key) -> 
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:get(BackendName, ProcessName, Table, Key).

-spec delete(Table :: table(), Key :: key()) -> error() | 'ok'.
delete(Table, Key) ->
    delete(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec delete(BackendName :: atom(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(BackendName, Table, Key) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:delete(BackendName, ProcessName, Table, Key).

-spec list_keys(Table :: table()) -> error() | {'ok', [key()]}.
list_keys(Table) ->
    list_keys(?DEFAULT_BACKEND_INSTANCE, Table).

-spec list_keys(BackendName :: atom(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(BackendName, Table) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:list_keys(BackendName, ProcessName, Table).

-spec list_buckets() -> error() | {'ok', [table()]}.
list_buckets() ->
    list_buckets(?DEFAULT_BACKEND_INSTANCE).

-spec list_buckets(BackendName :: atom()) -> error() | {'ok', [table()]}.
list_buckets(BackendName) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:list_buckets(BackendName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec process_name(BackendName :: atom()) -> cluster_name().
process_name(BackendName) ->
    kvdbc_cfg:backend_val(BackendName, process_name).

-spec module_name(BackendName :: atom()) -> atom().
module_name(BackendName) ->
    kvdbc_cfg:backend_val(BackendName, callback_module).
