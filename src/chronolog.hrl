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

%%
%%
-record(chronolog, {
   ticker  = undefined :: _
,  chunk   = undefined :: _
,  chronon = undefined :: tempus:t() | integer()
}).

%%
%% inverse time starting point
%%   16777215 = 1 bsl 24 - 1
%%    1048575 = 1 bsl 20 - 1
-define(T0, {16777215, 1048575, 1048575}).
