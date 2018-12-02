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
-module(chronolog_ticker).
-behaviour(pipe).

-include("chronolog.hrl").

-export([
   start_link/1
,  init/1
,  free/2
,  ioctl/2
,  handle/3
]).

-record(state, {
   chronolog = undefined :: #chronolog{}
}).

%%%----------------------------------------------------------------------------   
%%%
%%% Factory
%%%
%%%----------------------------------------------------------------------------   

start_link(#chronolog{ticker = Ticker} = Chronolog) ->
   pipe:start_link({via, pns, {urn, chronolog, Ticker}}, ?MODULE, [Chronolog], []).

init([Chronolog]) ->
   {ok, handle,
      #state{
         chronolog = Chronolog
      }
   }.

free(_, _) ->
   ok.

ioctl(chronolog, #state{chronolog = Chronolog}) ->
   Chronolog.

handle(_, _, #state{} = State) ->
   {next_state, handle, State}.
