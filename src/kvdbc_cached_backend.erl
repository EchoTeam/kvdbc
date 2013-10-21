%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_cached_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/2,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3
]).

-include_lib("riakc_cluster/include/riakc_cluster_types.hrl").

-type mcdkey() :: {atom(), cluster_name(), table(), key()}.
-type mcd_answer() :: {'error', term()} | {'ok', term()}.

-define(CACHE_KEY_EXPIRATION, 2*60). % 2 min

start_link(BackendName, ProcessName) ->
    Mod = module_name(BackendName),
    Mod:start_link(BackendName, ProcessName).

-spec put(BackendName :: atom(), ProcessName :: cluster_name(), Table :: table(), Key :: key(), Value :: value()) -> error() | 'ok'.
put(BackendName, ProcessName, Table, Key, Value) ->
    count(ProcessName, put),
    Mod = module_name(BackendName),
    PutResponse = Mod:put(BackendName, ProcessName, Table, Key, Value),
    case PutResponse of
        ok ->
            set_cached_value(BackendName, mcd_key(ProcessName,Table,Key), Value);
        {ok, _} ->
            set_cached_value(BackendName, mcd_key(ProcessName,Table,Key), Value);
        _ -> no_dice
    end,
    PutResponse.

-spec get(BackendName :: atom(), ProcessName :: cluster_name(), Table :: table(), Key :: key()) -> error() | {'ok', value()}.
get(BackendName, ProcessName, Table, Key) -> 
    MCDKey = mcd_key(ProcessName, Table, Key),
    case get_cached_value(BackendName, MCDKey) of
        {ok, CachedValue} ->
            count(ProcessName, get_cached),
            {ok,CachedValue};  %% returns cached value
        _ ->  
            count(ProcessName, get),
            Mod = module_name(BackendName),
            GetResponse = Mod:get(BackendName, ProcessName, Table, Key),
            case GetResponse of
                {ok, Value} -> set_cached_value(BackendName, MCDKey, Value);
                _   -> no_dice
            end,
            GetResponse
    end.

-spec delete(BackendName :: atom(), ProcessName :: cluster_name(), Table :: table(), Key :: key()) -> error() | 'ok'.
delete(BackendName, ProcessName, Table, Key) ->
    count(ProcessName, delete),
    Mod = module_name(BackendName),
    DelResponse = Mod:delete(BackendName, ProcessName, Table, Key),
    case DelResponse  of
        ok -> delete_cached_value(BackendName, mcd_key(ProcessName, Table, Key));
        _ ->  no_dice
    end,
    DelResponse.  %% We return result of riak del opration in any case

-spec list_keys(BackendName :: atom(), ProcessName :: cluster_name(), Table :: table()) -> error() | {'ok', [key()]}.
list_keys(BackendName, ProcessName, Table) ->
    Mod = module_name(BackendName),
    Mod:list_keys(BackendName, ProcessName, Table).

-spec list_buckets(BackendName :: atom(), ProcessName :: cluster_name()) -> error() | {'ok', [table()]}.
list_buckets(BackendName, ProcessName) ->
    Mod = module_name(BackendName),
    Mod:list_buckets(BackendName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec module_name(BackendName :: atom()) -> atom().
module_name(BackendName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    proplists:get_value(wrapped_backend_module, Config).

-spec cache_module(BackendName :: atom()) -> atom().
cache_module(BackendName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    proplists:get_value(cache_module, Config).

-spec mcd_key(ProcName :: cluster_name(), Table :: table(), Key :: key()) -> mcdkey().
mcd_key(ProcName, Table, Key) ->
    {?MODULE, ProcName, Table, Key}.

-spec get_cached_value(BackendName :: atom(), MCDKey :: mcdkey()) -> mcd_answer().
get_cached_value(BackendName, MCDKey) ->
    {Mod, ServerRef} = cache_module(BackendName),
    Mod:get(ServerRef, MCDKey).

-spec set_cached_value(BackendName :: atom(), MCDKey :: mcdkey(), Value :: value()) -> mcd_answer().
set_cached_value(BackendName, MCDKey, Value) ->
    {Mod, ServerRef} = cache_module(BackendName),
    Mod:set(ServerRef, MCDKey, Value, ?CACHE_KEY_EXPIRATION).

-spec delete_cached_value(BackendName :: atom(), MCDKey :: mcdkey()) -> mcd_answer().
delete_cached_value(BackendName, MCDKey) ->
    {Mod, ServerRef} = cache_module(BackendName),
    Mod:delete(ServerRef, MCDKey).

count(ProcessName, Op) ->
    case kvdbc_cfg:metrics_module() of
        undefined -> nop;
        Mod ->
            CounterName = {<<".">>, list_to_binary("riakc." ++ atom_to_list(ProcessName) ++ "." ++ atom_to_list(Op)), $r},
            Mod:safely_notify(CounterName, 1, meter)
    end.
