package kr.jay.batch.part3;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor
@Getter
public class Person {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int id;
	private String name;
	private String age;
	private String address;

	public Person(final String name, final String age, final String address) {
		this(0, name, age, address);
	}

	public Person(final int id, final String name, final String age, final String address) {
		this.id = id;
		this.name = name;
		this.age = age;
		this.address = address;
	}
}
