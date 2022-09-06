# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pytest

from clusterman.migration.settings import PoolPortion


@pytest.mark.parametrize(
    "initval,poolsize,result",
    (
        ("3%", 100, 3),
        (1, 100, 1),
        ("3%", 3, 1),
        (1, 3, 1),
        ("1", 3, 1),
    ),
)
def test_pool_portion(initval, poolsize, result):
    assert PoolPortion(initval).of(poolsize) == result


@pytest.mark.parametrize(
    "initval,exctype",
    (
        ("-3%", ValueError),
        (-1, ValueError),
        (0, ValueError),
        ("0%", ValueError),
        ("foobar", ValueError),
    ),
)
def test_pool_portion_error(initval, exctype):
    with pytest.raises(exctype):
        PoolPortion(initval)
