Feature: make sure the MesosPoolManager is requesting the right capacities

    Scenario Outline: initialization at minimum capacity
        Given a mesos pool manager with <rg_type>
         Then the resource groups should be at minimum capacity

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: balanced scale-up
        Given a mesos pool manager with <rg_type>
         When we request 53 capacity
         Then the resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: balanced scale-up with dry-run
        Given a mesos pool manager with <rg_type>
         When we request 53 capacity and dry-run is active
         Then the resource group capacities should not change

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: balanced scale-up after external modification
        Given a mesos pool manager with <rg_type>
          And the target capacity of the first resource group is 13
         When we request 76 capacity
         Then the resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: imbalanced scale-up
        Given a mesos pool manager with <rg_type>
          And the target capacity of the first resource group is 30
         When we request 1000 capacity
         Then the first resource group's capacity should not change
          And the remaining resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: balanced scale-down
        Given a mesos pool manager with <rg_type>
         When we request 1000 capacity
          And we request 80 capacity
         Then the resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: balanced scale-down with dry-run
        Given a mesos pool manager with <rg_type>
         When we request 1000 capacity
          And we request 80 capacity and dry-run is active
         Then the resource group capacities should not change

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |

    Scenario Outline: imbalanced scale-down
        Given a mesos pool manager with <rg_type>
         When we request 1000 capacity
          And the target capacity of the first resource group is 1
          And we request 22 capacity
         Then the first resource group's capacity should not change
          And the remaining resource groups should have evenly-balanced capacity

      Examples:
        | rg_type |
        | asgs    |
        | sfrs    |
