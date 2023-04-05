package kr.jay.batch.part6;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import kr.jay.batch.part4.UserRepository;

public class UserLevelUpPartitioner implements Partitioner {

	private final UserRepository userRepository;

	public UserLevelUpPartitioner(final UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	@Override
	public Map<String, ExecutionContext> partition(final int gridSize) {
		long mindId = userRepository.findMinId();
		long maxId = userRepository.findMaxId();

		long targetSize = (maxId - mindId) / gridSize + 1;

		Map<String, ExecutionContext> result = new HashMap<>();
		long number = 0;
		long start = mindId;
		long end = start + targetSize - 1;
		while (start <= maxId) {
			ExecutionContext value = new ExecutionContext();
			result.put("partition" + number, value);
			if (end >= maxId) {
				end = maxId;
			}
			value.putLong("minId", start);
			value.putLong("maxId", end);
			start += targetSize;
			end += targetSize;
			number++;
		}
		return result;
	}
}
