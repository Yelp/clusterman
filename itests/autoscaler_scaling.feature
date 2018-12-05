Feature: make sure the autoscaler scales to the proper amount

    Scenario Outline: make sure the autoscaler requests the right number of resources
       Given an autoscaler object
        When the signal resource request is <value>
        Then the autoscaler should scale rg1 to <rg1_target> capacity
         And the autoscaler should scale rg2 to <rg2_target> capacity

      Examples:
        | value     | rg1_target | rg2_target |
        | empty     | 10         | 10         |
        | 51 cpus   | 10         | 10         |
        | 56 cpus   | 10         | 10         |
        | 60 cpus   | 10         | 10         |
        | 70 cpus   | 13         | 12         |
        | 1000 cpus | 50         | 50         |
        | 42 cpus   | 8          | 8          |
        | 2 cpus    | 5          | 5          |

    Scenario Outline: make sure the autoscaler works on empty pools
       Given an autoscaler object
        When the pool is empty
         And the signal resource request is <value>
        Then the autoscaler should scale rg1 to <rg1_target> capacity
         And the autoscaler should scale rg2 to <rg2_target> capacity

      Examples:
        | value     | rg1_target | rg2_target |
        | 0 cpus    | 0          | 0          |
        | 20 cpus   | 1          | 0          |
