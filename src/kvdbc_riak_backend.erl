%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_riak_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/2,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3
]).
-export_type([
    errors/0,
    error/0,
    process_name/0
]).

-define(HANDLER_MODULE, 'riakc_cluster').

-type errors() :: ?HANDLER_MODULE:errors().
-type error() :: ?HANDLER_MODULE:error().
-type process_name() :: ?HANDLER_MODULE:cluster_name().
-type table() :: ?HANDLER_MODULE:table().
-type key() :: ?HANDLER_MODULE:key().
-type value() :: ?HANDLER_MODULE:value().
-type instance_name() :: kvdbc:instance_name().

start_link(InstanceName, ProcessName) ->
    Config = kvdbc_cfg:backend_val(InstanceName, config),
    riakc_cluster:start_link(ProcessName, Config).

-spec put(_InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(_InstanceName, ProcessName, Table, Key, Value) ->
    riakc_cluster:put(ProcessName, Table, Key, Value, [{w, 2}]).

-spec get(_InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(_InstanceName, ProcessName, Table, Key) ->
    riakc_cluster:get(ProcessName, Table, Key, [{r, 2}]).

-spec delete(_InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(_InstanceName, ProcessName, Table, Key) ->
    riakc_cluster:delete(ProcessName, Table, Key, [{rw, 2}]).

-spec list_keys(_InstanceName :: instance_name(), ProcessName :: process_name(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(_InstanceName, ProcessName, Table) ->
    riakc_cluster:list_keys(ProcessName, Table).

-spec list_buckets(_InstanceName :: instance_name(), ProcessName :: process_name()) -> error() | {'ok', [table()]}.
list_buckets(_InstanceName, ProcessName) ->
    riakc_cluster:list_buckets(ProcessName).
