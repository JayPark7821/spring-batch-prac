package kr.jay.batch.part4;

import java.time.LocalDate;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
	List<User> findAllByUpdateDate(LocalDate updateDate);

	@Query("select min(u.id) from User u ")
	long findMinId();

	@Query("select max(u.id) from User u ")
	long findMaxId();
}
