package com.kodilla.csvconverterv2;

import com.kodilla.csvconverterv2.domain.User;
import com.kodilla.csvconverterv2.domain.UserDto;
import org.springframework.batch.item.ItemProcessor;

import java.time.LocalDate;
import java.time.Period;

public class UserProcessor implements ItemProcessor<UserDto, User> {

    @Override
    public User process(UserDto item) throws Exception {
        return new User(item.getFirstName(), item.getLastName(), getAgeFromDate(item.getDateOfBirth()));
    }

    private int getAgeFromDate(LocalDate birthDate){
        return  Period.between(birthDate, LocalDate.now()).getYears();
    }
}
