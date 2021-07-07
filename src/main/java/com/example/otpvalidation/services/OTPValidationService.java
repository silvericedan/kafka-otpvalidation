package com.example.otpvalidation.services;

import com.example.otpvalidation.bindings.OTPListenerBinding;
import com.example.otpvalidation.model.PaymentConfirmation;
import com.example.otpvalidation.model.PaymentRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;


@Log4j2
@Service
@EnableBinding(OTPListenerBinding.class)
public class OTPValidationService {

    @Autowired
    private RecordBuilder recordBuilder;


    //In this case we do not define the input channel using the stream listener annotation
    //we define the @Input annotations in the process method arguments
    @StreamListener
    public void process(@Input("payment-request-channel") KStream<String, PaymentRequest> request,
                        @Input("payment-confirmation-channel") KStream<String, PaymentConfirmation> confirmation) {

        //print the request key and time
        request.foreach((k, v) -> log.info("Request Key = " + k + " Created Time = "
                + Instant.ofEpochMilli(v.getCreatedTime()).atOffset(ZoneOffset.UTC)));

        //print the confirmation key and time
        confirmation.foreach((k, v) -> log.info("Confirmation Key = " + k + " Created Time = "
                + Instant.ofEpochMilli(v.getCreatedTime()).atOffset(ZoneOffset.UTC)));

        //we use the join method in the request, and the first argument is
        //the other stream we want to join, the second argument is a valuejoiner lambda
        //where the "r" argument is the record of the left side, the PaymentRequest
        //and the "c" argument is the PaymentConfirmation
        //the lambda method is triggered by the framework only if the matching record is found
        //third argument is the JoinWindow used to define the time constraint, if any part of
        //the record is missing or do not appear into the 5 minute window, valuejoiner is not triggered
        //fourth argument is to define the required Serdes
        request.join(confirmation,
                (r, c) -> recordBuilder.getTransactionStatus(r, c),
                JoinWindows.of(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(),
                        new JsonSerde<>(PaymentRequest.class),
                        new JsonSerde<>(PaymentConfirmation.class)))
                .foreach((k, v) -> log.info("Transaction ID = " + k + " Status = " + v.getStatus()));

    }
}
