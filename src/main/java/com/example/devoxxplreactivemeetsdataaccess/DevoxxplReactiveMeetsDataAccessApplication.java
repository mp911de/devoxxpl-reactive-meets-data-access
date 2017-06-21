package com.example.devoxxplreactivemeetsdataaccess;

import java.time.Duration;
import java.util.Random;
import java.util.stream.Stream;

import io.reactivex.Flowable;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.repository.Tailable;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RequiredArgsConstructor
public class DevoxxplReactiveMeetsDataAccessApplication implements CommandLineRunner{

	final ReactiveMongoOperations reactiveMongoOperations;
	final MongoOperations mongoOperations;

	public static void main(String[] args) {
		SpringApplication.run(DevoxxplReactiveMeetsDataAccessApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {

		mongoOperations.createCollection(Phrase.class, CollectionOptions.empty()
				.capped().size(10000));

		String phrases[] = {"Czesc", "Dzienkuje", "Piwo", "Nazdrowie"};

		Stream<Phrase> stream = Stream.generate(() -> phrases[new Random().nextInt(phrases.length)])
				.map(Phrase::new);

		Flux.fromStream(stream)
				.zipWith(Flux.interval(Duration.ofSeconds(2)))
				.map(Tuple2::getT1)
				.flatMap(reactiveMongoOperations::save)
				.doOnNext(System.out::println)
				.subscribe();

		System.out.println("Happily saving phrases \\ö/");
	}
}

interface PhraseRepository extends ReactiveCrudRepository<Phrase, String> {

	Flowable<Phrase> findAllByPhraseStartingWith(String prefix);

	@Tailable
	Flux<Phrase> findAllBy();
}

@RestController
@RequiredArgsConstructor
class PhraseController {

	final PhraseRepository repository;

	@GetMapping("phrases")
	Flux<Phrase> getPhrases(){
		return repository.findAll();
	}

	@GetMapping("phrases/by-prefix")
	Flowable<Phrase> getPhrases(@RequestParam String prefix){
		return repository.findAllByPhraseStartingWith(prefix);
	}

	@GetMapping(value = "phrases/stream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
	Flux<Phrase> stream(){
		return repository.findAllBy();
	}
}

@Data
@RequiredArgsConstructor
class Phrase {

	String id;
	final String phrase;
}
