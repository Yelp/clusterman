min_capacity = 1
max_capacity = 2560
target_capacity = 1126
requested_target_capacity = 0
max_tasks_to_kill =  100
max_weight_to_add = 430
max_weight_to_remove = 100

while True:
	requested_delta = requested_target_capacity - target_capacity
	if requested_delta > 0:
		delta = min(max_capacity - target_capacity, requested_delta)
	elif requested_delta < 0:
       delta = max(min_capacity - target_capacity, requested_delta)
    else:
    	delta = 0

    if delta > 0:
    	delta = min(max_weight_to_add, delta)
    elif delta < 0:
    	delta = max(-max_weight_to_remove, delta)
    print(f"The target_capacity has been changed from {target_capacity} to {target_capacity + delta}")
    target_capacity = target_capacity + delta
