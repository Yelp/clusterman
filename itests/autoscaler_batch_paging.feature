Feature: make sure the autoscaler pages the right people

    Scenario Outline: signal initialization is broken
        Given the autoscaler batch
         When initialization of the <signal_type> signal fails
         Then the application owner should <warn_application?> for initialization
          And the service owner should <page_service?> for initialization

      Examples: Signal Types
        | signal_type | warn_application? | page_service? |
        | application | get warned        | not get paged |
        | default     | not get warned    | get paged     |

    Scenario: the autoscaler is broken
        Given the autoscaler batch
         When initialization succeeds
          And the autoscaler fails
         Then initialization should not page
          And the application owner should not get paged for evaluation
          And the service owner should get paged for evaluation

    Scenario Outline: a signal is broken
        Given the autoscaler batch
         When initialization succeeds
          And the <signal_type> signal fails
         Then initialization should not page
          And the application owner should <page_application?> for evaluation
          And the service owner should <page_service?> for evaluation

      Examples: Signal Types
        | signal_type | page_application? | page_service? |
        | application | get paged         | not get paged |
        | default     | not get paged     | get paged     |

    Scenario: everything is fine
        Given the autoscaler batch
         When initialization succeeds
          And signal evaluation succeeds
         Then initialization should not page
          And the application owner should not get paged for evaluation
          And the service owner should not get paged for evaluation

    Scenario: RequestLimitExceeded errors are ignored
        Given the autoscaler batch
         When initialization succeeds
          And a RequestLimitExceeded error occurs
         Then initialization should not page
          And the application owner should not get paged for evaluation
          And the service owner should not get paged for evaluation
