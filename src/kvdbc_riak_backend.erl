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

-type error() :: {'error', 'notfound' | {'riak', riakc_cluster:errors()}}.
-type process_name() :: riakc_cluster:cluster_name().
-type table() :: riakc_cluster:table().
-type key() :: riakc_cluster:key().
-type value() :: riakc_cluster:value().

-define(HANDLER_MODULE, 'riakc_cluster').

start_link(BackendName, ProcessName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    ?HANDLER_MODULE:start_link(ProcessName, Config).

-spec put(_BackendName :: term(), ProcessName :: process_name(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(_BackendName, ProcessName, Table, Key, Value) ->
    kvdbc_handler_call(put, [ProcessName, Table, Key, Value, [{w, 2}]]).

-spec get(_BackendName :: term(), ProcessName :: process_name(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(_BackendName, ProcessName, Table, Key) -> 
    kvdbc_handler_call(get, [ProcessName, Table, Key, [{r, 2}]]).

-spec delete(_BackendName :: term(), ProcessName :: process_name(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(_BackendName, ProcessName, Table, Key) ->
    kvdbc_handler_call(delete, [ProcessName, Table, Key, [{rw, 2}]]).

-spec list_keys(_BackendName :: term(), ProcessName :: process_name(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(_BackendName, ProcessName, Table) ->
    kvdbc_handler_call(list_keys, [ProcessName, Table]).

-spec list_buckets(_BackendName :: term(), ProcessName :: process_name()) -> error() | {'ok', [table()]}.
list_buckets(_BackendName, ProcessName) ->
    kvdbc_handler_call(list_buckets, [ProcessName]).

% private functions

kvdbc_handler_call(Func, Args) ->
    case erlang:apply(?HANDLER_MODULE, Func, Args) of
        {error, notfound} ->
            {error, notfound};
        {error, Reason} ->
            {error, {riak, Reason}};
        Any ->
            Any
    end.
