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

-export_type([
    errors/0,
    table/0,
    key/0,
    value/0
]).

-define(DEFAULT_BACKEND_INSTANCE, default).

-include_lib("riakc_cluster/include/riakc_cluster_types.hrl").

-spec put(Table::binary(), Key::term(), Value::term()) ->
    ok | {error, term()}.
put(Table, Key, Value) ->
    put(?DEFAULT_BACKEND_INSTANCE, Table, Key, Value).

-spec put(BackendName::atom(), Table::binary(), Key::term(), Value::term()) ->
    ok | {error, term()}.
put(BackendName, Table, Key, Value) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:put(BackendName, ProcessName, Table, Key, Value).

-spec get(Table::binary(), Key::term()) ->
    {ok, term()} | {error, term()}.
get(Table, Key) ->
    get(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec get(BackendName::atom(), Table::binary(), Key::term()) ->
    {ok, term()} | {error, term()}.
get(BackendName, Table, Key) -> 
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:get(BackendName, ProcessName, Table, Key).

-spec delete(Table::binary(), Key::term()) ->
    ok | {error, term()}.
delete(Table, Key) ->
    delete(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec delete(BackendName::atom(), Table::binary(), Key::term()) ->
    ok | {error, term()}.
delete(BackendName, Table, Key) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:delete(BackendName, ProcessName, Table, Key).

-spec list_keys(Table::binary()) -> {ok, [binary()]} | {error, term()}.
list_keys(Table) -> list_keys(?DEFAULT_BACKEND_INSTANCE, Table).

-spec list_keys(BackendName::atom(), Table::binary()) ->
    {ok, [binary()]} | {error, term()}.
list_keys(BackendName, Table) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:list_keys(BackendName, ProcessName, Table).

-spec list_buckets() -> {ok, [binary()]} | {error, term()}.
list_buckets() ->
    list_buckets(?DEFAULT_BACKEND_INSTANCE).

-spec list_buckets(BackendName::atom()) ->
    {ok, [binary()]} | {error, term()}.
list_buckets(BackendName) ->
    ProcessName = process_name(BackendName),
    Mod = module_name(BackendName),
    Mod:list_buckets(BackendName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

process_name(BackendName) ->
    kvdbc_cfg:backend_val(BackendName, process_name).

module_name(BackendName) ->
    kvdbc_cfg:backend_val(BackendName, callback_module).
