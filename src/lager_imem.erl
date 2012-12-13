-module(lager_imem).
-behaviour(gen_event).

%% gen_event callbacks
-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([trace/1, trace/2]).
-export([test/0]).

-record(state, {
        level=info,
        table,
        user,
        password,
        db,
        session}).

-record(imemLog, {
          datetime,
	      level,
	      pid,
	      module,
	      function,
	      line,
	      node,
	      message}).
-define(imemLog, [list, list, list, list, list, integer, list, list]).

%%%===================================================================
%%% trace
%%%===================================================================
trace(Filter) ->
    trace(Filter, debug).
trace(Filter, Level) ->
    Trace0 = {Filter, Level, ?MODULE},
    case lager_util:validate_trace(Trace0) of
        {ok, Trace} ->
            {MinLevel, Traces} = lager_config:get(loglevel),
            case lists:member(Trace, Traces) of
                false ->
                    lager_config:set(loglevel, {MinLevel, [Trace|Traces]});
                _ ->
                    ok
            end,
            {ok, Trace};
        Error ->
            Error
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================
init(Params) ->
    application:start(imem),
    try
        State = state_from_params(#state{}, Params),
        Password = erlang:md5(State#state.password),
        Cred = {State#state.user, Password},
        ImemSession = erlimem_session:open(local, {State#state.db}, Cred),
        Table = State#state.table,
        ImemSession:run_cmd(create_table, [Table, {record_info(fields, imemLog), ?imemLog, setelement(1, #imemLog{}, Table)}, [{local_content, true}, {type, bag}], mpro]),
        ImemSession:run_cmd(check_table, [Table]),
        {ok, State#state{session=ImemSession}}
    catch
        Class:Reason -> io:format(user, "~p failed with ~p:~p~n", [?MODULE,Class, Reason]),
            {stop, "Cant create required lager imem resources"}
    end.

handle_event({log, LagerMsg}, State = #state{table=Table, session=ImemSession, level = LogLevel}) ->
    case lager_util:is_loggable(LagerMsg, LogLevel, ?MODULE) of
        true ->
            Level = lager_msg:severity_as_int(LagerMsg),
            {Date, Time} = lager_msg:timestamp(LagerMsg),
            Message = lager_msg:message(LagerMsg),
            Metadata = lager_msg:metadata(LagerMsg),
            Mod = proplists:get_value(module, Metadata),
            Fun = proplists:get_value(function, Metadata),
            Line = proplists:get_value(line, Metadata),
            Pid = proplists:get_value(pid, Metadata),

            Entry = #imemLog{
                    datetime = {lists:flatten(Date), lists:flatten(Time)},
                    level = lager_util:num_to_level(Level),
                    message = lists:flatten(Message),
                    module = Mod,
                    function = Fun,
                    line = Line,
                    pid = Pid},

            ImemSession:run_cmd(write, [Table, setelement(1, Entry, Table)]);
        false ->
            ok
    end,
    {ok, State};

handle_event({lager_imem_options, Params}, State) ->
    {ok, state_from_params(State, Params)};

handle_event(_Event, State) ->
    {ok, State}.

handle_call({set_loglevel, Level}, State) ->
    {ok, ok, State#state{level = lager_util:level_to_num(Level) }};

handle_call(get_loglevel, State = #state{level = Level}) ->
    {ok, Level, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
state_from_params(OrigState = #state{user = OldUser,
                                     password = OldPassword,
                                     level = OldLevel,
                                     db = OldDb,
                                     table = OldTable}, Params) ->
    User = proplists:get_value(user, Params, OldUser),
    Password = proplists:get_value(password, Params, OldPassword),
    Db = proplists:get_value(db, Params, OldDb),
    Table = proplists:get_value(table, Params, OldTable),
    Level = proplists:get_value(level, Params, OldLevel),

    OrigState#state{level=lager_util:level_to_num(Level),
                    user=User,
                    password=Password,
                    db=Db,
                    table=Table}.

%%%===================================================================
%%% Tests
%%%===================================================================

test() ->
    application:load(lager),
    application:set_env(lager, handlers, [{lager_console_backend, debug},
                                          {lager_imem, [{db, "MproLog"},
                                                        {table, test},
                                                        {level, debug},
                                                        {user, <<"admin">>},
                                                        {password, <<"change_on_install">>}]},
                                          {lager_file_backend,
                                           [{"error.log", error, 10485760, "$D0", 5},
                                            {"console.log", info, 10485760, "$D0", 5}]}]),
    application:set_env(lager, error_logger_redirect, false),
    lager:start(),
    lager:log(info, self(), "Test INFO message"),
    lager:log(debug, self(), "Test DEBUG message"),
    lager:log(error, self(), "Test ERROR message"),
    lager:warning([{a,b}, {c,d}], "Hello", []),
    lager:info("Info ~p", ["variable"]).
