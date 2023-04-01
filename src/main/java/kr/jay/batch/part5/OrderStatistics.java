package kr.jay.batch.part5;

import java.time.LocalDate;

import lombok.Builder;
import lombok.Getter;

@Getter
public class OrderStatistics {

	private String amount;
	private LocalDate date;

	@Builder
	public OrderStatistics(final String amount, final LocalDate date) {
		this.amount = amount;
		this.date = date;
	}
}
