%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(kvdbc_local_backend).

-behaviour(kvdbc_backend).
-behaviour(gen_server).

-export([
    start_link/1,
    create_bucket/3,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3,
    stop/1
]).

-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

start_link(InstanceName) ->
    gen_server:start_link({local, InstanceName}, ?MODULE, [], []).

stop(InstanceName) ->
    gen_server:call(InstanceName, {stop}).

put(InstanceName, Table, Key, Value, _Opts) ->
    gen_server:call(InstanceName, {put, Table, Key, Value}).

get(InstanceName, Table, Key, _Opts) ->
    gen_server:call(InstanceName, {get, Table, Key}).

delete(InstanceName, Table, Key, _Opts) ->
    gen_server:call(InstanceName, {delete, Table, Key}).

list_keys(InstanceName, Table, _Opts) ->
    gen_server:call(InstanceName, {list_keys, Table}).

list_buckets(InstanceName, _Opts) ->
    gen_server:call(InstanceName, {list_buckets}).

create_bucket(_InstanceName, BucketName, _Opts) ->
    {ok, BucketName}.

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
