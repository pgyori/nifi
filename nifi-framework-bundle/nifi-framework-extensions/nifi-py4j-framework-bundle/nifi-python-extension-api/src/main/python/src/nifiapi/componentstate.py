#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from enum import Enum
from nifiapi.__jvm__ import JvmHolder

CLUSTER_SCOPE = JvmHolder.jvm.org.apache.nifi.components.state.Scope.CLUSTER
LOCAL_SCOPE = JvmHolder.jvm.org.apache.nifi.components.state.Scope.LOCAL

def convert_to_java_scope(scope):
    if scope == Scope.LOCAL:
        return LOCAL_SCOPE
    else:
        return CLUSTER_SCOPE

def convert_dict_to_map(dictionary):
    java_map = JvmHolder.jvm.java.util.HashMap()
    for key, value in dictionary.items():
        java_map.put(key, value)
    return java_map

class Scope(Enum):
    CLUSTER = 1
    LOCAL = 2


class StateManager:

    _java_state_manager = None

    def __init__(self, java_state_manager):
        self._java_state_manager = java_state_manager

    def setState(self, state, scope):
        self._java_state_manager.setState(convert_dict_to_map(state), convert_to_java_scope(scope))

    def getState(self, scope):
        return self._java_state_manager.getState(convert_to_java_scope(scope))

    def replace(self, oldValue, newValue, scope):
        self._java_state_manager.replace(oldValue, convert_dict_to_map(newValue), convert_to_java_scope(scope))

    def clear(self, scope):
        self._java_state_manager.clear(convert_to_java_scope(scope))