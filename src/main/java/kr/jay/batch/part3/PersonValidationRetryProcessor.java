package kr.jay.batch.part3;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {

	private final RetryTemplate retryTemplate;

	public PersonValidationRetryProcessor() {
		this.retryTemplate = new RetryTemplateBuilder()
			.maxAttempts(3)
			.retryOn(NotFoundNameException.class)
			.withListener(new SavePersonRetryListener())
			.build();

	}
	@Override
	public Person process(final Person item) throws Exception {

		return this.retryTemplate.execute(context ->{
			//retryCallback
			if(item.isNotEmptyName()){
				return item;
			}
			throw new NotFoundNameException();
		}, context -> {
			//recoveryCallback
			return item.unknownName();
		});
	}

	private class SavePersonRetryListener implements RetryListener {
		@Override
		public <T, E extends Throwable> boolean open(final RetryContext context, final RetryCallback<T, E> callback) {
			return true;
		}

		@Override
		public <T, E extends Throwable> void close(final RetryContext context, final RetryCallback<T, E> callback,
			final Throwable throwable) {
			log.info("close");
		}

		@Override
		public <T, E extends Throwable> void onError(final RetryContext context, final RetryCallback<T, E> callback,
			final Throwable throwable) {
			log.info("onError");
		}
	}
}
