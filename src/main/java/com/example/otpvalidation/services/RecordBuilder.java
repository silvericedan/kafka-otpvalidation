package com.example.otpvalidation.services;

import com.example.otpvalidation.model.PaymentConfirmation;
import com.example.otpvalidation.model.PaymentRequest;
import com.example.otpvalidation.model.TransactionStatus;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RecordBuilder {
    public TransactionStatus getTransactionStatus(PaymentRequest request, PaymentConfirmation confirmation){
        String status = "Failure";
        if(request.getOTP().equals(confirmation.getOTP()))
            status = "Success";

        TransactionStatus transactionStatus = new TransactionStatus();
        transactionStatus.setTransactionID(request.getTransactionID());
        transactionStatus.setStatus(status);
        return transactionStatus;
    }
}
