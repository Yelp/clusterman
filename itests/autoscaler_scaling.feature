Feature: make sure the autoscaler scales to the proper amount

    # The setpoint here is 0.7 with a margin of +/- 0.1 (see environment.py), and we're assuming that
    # we were exactly at the setpoint of 0.7 * 80 = 56.
    # Thus the window of CPU resource requests for which the autoscaler won't make changes is
    # [51, 61] CPUs (inclusive)  (51 / 56 = 0.91, 50 / 56 = 0.90, 61 / 56 = 1.089 62 / 56 = 1.107)
    #
    # We have min_capacity = 3, max_capacity = 100, max_weight_to_add = 200, max_weight_to_remove = 10
    Scenario Outline: make sure the autoscaler requests the right number of resources
       Given a cluster with 2 resource groups
         And 20 target capacity
         And 80 CPUs, 1000 MB mem, 1000 MB disk, and 0 GPUs
         And a mesos autoscaler object
        When the signal resource request is <value>
         And the autoscaler runs
        Then no exception is raised
         And the autoscaler should scale rg1 to <rg1_target> capacity
         And the autoscaler should scale rg2 to <rg2_target> capacity

      Examples:
        | value     | rg1_target | rg2_target |
        | empty     | 10         | 10         |
        | 51 cpus   | 10         | 10         |
        | 56 cpus   | 10         | 10         |
        | 61 cpus   | 10         | 10         |
        | 70 cpus   | 13         | 12         |
        | 1000 cpus | 50         | 50         |
        | 42 cpus   | 8          | 8          |
        | 2 cpus    | 5          | 5          |
        | 0 gpus    | 5          | 5          |

    Scenario Outline: make sure the autoscaler works on empty pools
       Given a cluster with 2 resource groups
         And 20 target capacity
         And 80 CPUs, 1000 MB mem, 1000 MB disk, and 0 GPUs
         And a mesos autoscaler object
        When the pool is empty
         And metrics history <exists>
         And the signal resource request is <value>
         And the autoscaler runs
        Then no exception is raised
         And the autoscaler should scale rg1 to <rg1_target> capacity
         And the autoscaler should scale rg2 to <rg2_target> capacity

      Examples:
        | value     | rg1_target | rg2_target | exists |
        | 0 cpus    | 0          | 0          |     no |
        | 20 cpus   | 1          | 0          |     no |
        | 20 cpus   | 21         | 20         |    yes |

    Scenario: the autoscaler does nothing when it is paused
       Given a cluster with 2 resource groups
         And 20 target capacity
         And 80 CPUs, 1000 MB mem, 1000 MB disk, and 0 GPUs
         And a mesos autoscaler object
        When the autoscaler is paused
         And the signal resource request is 1000 cpus
        Then no exception is raised
         And the autoscaler should do nothing

    Scenario Outline: the default PendingPodsSignal works correctly
       Given a cluster with 2 resource groups
         And 20 target capacity
         And 80 CPUs, 1000 MB mem, 1000 MB disk, and 0 GPUs
         And 56 CPUs allocated and <pending> CPUs pending
         And a kubernetes autoscaler object
        When the autoscaler runs
        Then no exception is raised
         And the autoscaler should scale rg1 to <rg1_target> capacity
         And the autoscaler should scale rg2 to <rg2_target> capacity

      Examples:
        | pending   | rg1_target | rg2_target |
        | 0         | 10         | 10         |
        | 14        | 16         | 15         |
        | 1000      | 50         | 50         |

    @wip
    Scenario: the PendingPodsSignal is using up-to-date data
       Given a cluster with 1 resource group
         And 10 target capacity
         And 40 CPUs, 500 MB mem, 500 MB disk, and 0 GPUs
         And 28 CPUs allocated and 0 CPUs pending
         And a kubernetes autoscaler object
        When the autoscaler runs only once
         And allocated CPUs changes to 40
         And the autoscaler runs only once
        Then no exception is raised
         And the autoscaler should scale rg1 to 15 capacity


    Scenario: instances are not killed if we've lost capacity recently
       Given a cluster with 1 resource group
         And 10 target capacity
         And 40 CPUs, 500 MB mem, 500 MB disk, and 0 GPUs
         And 1 CPUs allocated and 0 CPUs pending
         And a kubernetes autoscaler object with prevent_scale_down_after_capacity_loss enabled
        When the cluster has recently lost capacity
         And the autoscaler runs only once
        Then no exception is raised
         And the autoscaler should do nothing
