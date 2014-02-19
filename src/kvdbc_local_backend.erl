%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(kvdbc_local_backend).

-behaviour(kvdbc_backend).
-behaviour(gen_server).

-export([
    start_link/2,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3,
    stop/2
]).

-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

start_link(_InstanceName, ProcessName) ->
    gen_server:start_link({local, ProcessName}, ?MODULE, [], []).

stop(_InstanceName, ProcessName) ->
    gen_server:call(ProcessName, {stop}).

put(_InstanceName, ProcessName, Table, Key, Value) ->
    gen_server:call(ProcessName, {put, Table, Key, Value}).

get(_InstanceName, ProcessName, Table, Key) ->
    gen_server:call(ProcessName, {get, Table, Key}).

delete(_InstanceName, ProcessName, Table, Key) ->
    gen_server:call(ProcessName, {delete, Table, Key}).

list_keys(_InstanceName, ProcessName, Table) ->
    gen_server:call(ProcessName, {list_keys, Table}).

list_buckets(_InstanceName, ProcessName) ->
    gen_server:call(ProcessName, {list_buckets}).

%%%% gen_server callbacks

init([]) ->
    {ok, undefined}.

handle_call({put, Table, Key, Value}, _From, State) ->
    put({Table, Key}, Value),
    {reply, ok, State};

handle_call({get, Table, Key}, _From, State) ->
    Value = get({Table, Key}),
    {reply, {ok, Value}, State};

handle_call({delete, Table, Key}, _From, State) ->
    erase({Table, Key}),
    {reply, ok, State};

handle_call({list_keys, Table}, _From, State) ->
    Keys = [Key || {{StorageTable, Key}, _Value} <- get(), StorageTable =:= Table],
    {reply, {ok, Keys}, State};

handle_call({list_buckets}, _From, State) ->
    Buckets = lists:usort([Table || {{Table, _Key}, _Value} <- get()]),
    {reply, {ok, Buckets}, State};

handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
