%%
%%   Copyright (c) 2011 - 2015, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(chronolog_cask).
-behaviour(gen_server).
-include("chronolog.hrl").

-export([
   start_link/1
  ,init/1
  ,terminate/2
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
]).

%%
%% internal state
-record(srv, {
   fd      = undefined :: any()
  ,chronon = {0, 0, 1} :: tempus:t() | integer()
  ,owner   = undefined :: pid()
  ,opts    = undefined :: list()
}).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(Opts) ->
   gen_server:start_link(?MODULE, [Opts], []).

init([Opts]) ->
   {ok, init(Opts, #srv{opts=Opts})}.

init([{owner, X} | Opts], State) ->
   erlang:monitor(process, X),
   init(Opts, State#srv{owner=X});
init([{chronon, X} | Opts], State) ->
   init(Opts, State#srv{chronon=X});
init([_ | Opts], State) ->
   init(Opts, State);
init([], #srv{chronon=C, opts=Opts}=State) ->
   {ok, FD} = dive:new(Opts),
   State#srv{fd = #chronolog{fd=FD, chronon=C}}.
   
terminate(_Reason, #srv{fd = #chronolog{fd = FD}}) ->
   dive:free(FD).

%%%----------------------------------------------------------------------------   
%%%
%%% gen_server
%%%
%%%----------------------------------------------------------------------------   

%%
%%
handle_call(i, _Tx, #srv{fd=FD}=State) ->
   {reply, {ok, FD}, State};

handle_call(free, _Tx, State) ->
   {stop, normal, ok, State};

handle_call(_Req, _Tx, State) ->
   {noreply, State}.

%%
%%
handle_cast(_Req, State) ->
   {noreply, State}.

%%
%%
handle_info({'DOWN', _, _Type, Owner, _Reason}, #srv{owner=Owner}=State) ->
   {stop, normal, State};

handle_info(_Req, State) ->
   {noreply, State}.

%%
%%
code_change(_Vsn, State, _Extra) ->
   {ok, State}.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   
