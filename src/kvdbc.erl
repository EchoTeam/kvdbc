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
    error/0,
    process_name/0,
    instance_name/0,
    table/0,
    key/0,
    value/0
]).

-define(DEFAULT_BACKEND_INSTANCE, default).

-type errors() :: kvdbc_riak_backend:errors().
-type error() :: kvdbc_riak_backend:error().
-type process_name() :: kvdbc_riak_backend:process_name().
-type instance_name() :: atom().
-type table() :: binary().
-type key() :: binary().
-type value() :: term().

-spec put(Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(Table, Key, Value) ->
    put(?DEFAULT_BACKEND_INSTANCE, Table, Key, Value).

-spec put(InstanceName :: instance_name(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(InstanceName, Table, Key, Value) ->
    ProcessName = process_name(InstanceName),
    Mod = module_name(InstanceName),
    Mod:put(InstanceName, ProcessName, Table, Key, Value).

-spec get(Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(Table, Key) ->
    get(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec get(InstanceName :: instance_name(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(InstanceName, Table, Key) ->
    ProcessName = process_name(InstanceName),
    Mod = module_name(InstanceName),
    Mod:get(InstanceName, ProcessName, Table, Key).

-spec delete(Table :: table(), Key :: key()) -> error() | 'ok'.
delete(Table, Key) ->
    delete(?DEFAULT_BACKEND_INSTANCE, Table, Key).

-spec delete(InstanceName :: instance_name(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(InstanceName, Table, Key) ->
    ProcessName = process_name(InstanceName),
    Mod = module_name(InstanceName),
    Mod:delete(InstanceName, ProcessName, Table, Key).

-spec list_keys(Table :: table()) -> error() | {'ok', [key()]}.
list_keys(Table) -> list_keys(?DEFAULT_BACKEND_INSTANCE, Table).

-spec list_keys(InstanceName :: instance_name(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, Table) ->
    ProcessName = process_name(InstanceName),
    Mod = module_name(InstanceName),
    Mod:list_keys(InstanceName, ProcessName, Table).

-spec list_buckets() -> error() | {'ok', [table()]}.
list_buckets() ->
    list_buckets(?DEFAULT_BACKEND_INSTANCE).

-spec list_buckets(InstanceName :: instance_name()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName) ->
    ProcessName = process_name(InstanceName),
    Mod = module_name(InstanceName),
    Mod:list_buckets(InstanceName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec process_name(InstanceName :: instance_name()) -> process_name().
process_name(InstanceName) ->
    kvdbc_cfg:backend_val(InstanceName, process_name).

-spec module_name(InstanceName :: instance_name()) -> atom().
module_name(InstanceName) ->
    kvdbc_cfg:backend_val(InstanceName, callback_module).
