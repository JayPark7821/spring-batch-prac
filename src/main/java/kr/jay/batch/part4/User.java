package kr.jay.batch.part4;

import java.time.LocalDate;
import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor
public class User {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String username;

	@Enumerated(EnumType.STRING)
	private Level level = Level.NORMAL;

	private int totalAmount;

	private LocalDate updateDate;

	public User(final String username, final int totalAmount) {
		this.username = username;
		this.totalAmount = totalAmount;
	}

	public boolean availableLevelUp() {
		return Level.availableLevelUp(this.level, this.totalAmount);
	}

	public Level levelUp() {
		Level nextLevel = Level.getNextLevel(this.totalAmount);
		this.level = nextLevel;
		this.updateDate = LocalDate.now();
		return nextLevel;
	}

	public enum Level {
		VIP(500_000, null),
		GOLD(500_000, VIP),
		SILVER(300_000, GOLD),
		NORMAL(200_000, SILVER);

		private final int nextAmount;
		private final Level nextLevel;

		Level(int nextAmount, Level nextLevel) {
			this.nextAmount = nextAmount;
			this.nextLevel = nextLevel;
		}

		private static boolean availableLevelUp(final Level level, final int totalAmount) {
			if (Objects.isNull(level)) {
				return false;
			}

			if(Objects.isNull(level.nextLevel)) {
				return false;
			}

			return totalAmount >= level.nextAmount;
		}

		private static Level getNextLevel(final int totalAmount) {
			if(totalAmount >= VIP.nextAmount) {
				return VIP;
			}
			if(totalAmount >= GOLD.nextAmount) {
				return GOLD.nextLevel;
			}
			if(totalAmount >= SILVER.nextAmount) {
				return SILVER.nextLevel;
			}
			if(totalAmount >= NORMAL.nextAmount) {
				return NORMAL.nextLevel;
			}
			return NORMAL;
		}
	}
}