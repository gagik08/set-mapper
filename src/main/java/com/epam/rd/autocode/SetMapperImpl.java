package com.epam.rd.autocode;

import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

public class SetMapperImpl implements SetMapper<Set<Employee>> {
    private static final Logger LOGGER = LogManager.getLogger(SetMapperImpl.class);
    private static final String ID = "id";
    private static final String SALARY = "salary";
    private static final String HIREDATE = "hiredate";
    private static final String FIRSTNAME = "firstname";
    private static final String LASTNAME = "lastname";
    private static final String MIDDLENAME = "middlename";
    private static final String POSITION = "position";
    private static final String MANAGER = "manager";

    @Override
    public Set<Employee> mapSet(ResultSet resultSet) {
        Set<Employee> employees = new HashSet<>();
        try {
            while (resultSet.next()) {
                Employee employee = getEmployee(resultSet);
                employees.add(employee);
            }
        } catch (SQLException e) {
            LOGGER.error("Something went wrong at mapSet!!!", e);
            throw new RuntimeException(e);
        }
        return employees;
    }

    private Employee getEmployee(ResultSet resultSet) {
        try {
            int managerId = resultSet.getInt(MANAGER);
            BigInteger id = new BigInteger(resultSet.getString(ID));
            FullName fullName = getFullName(resultSet);
            Position position = getPosition(resultSet);
            LocalDate hiredate = getHiredate(resultSet);
            BigDecimal salary = resultSet.getBigDecimal(SALARY);
            Employee manager = getManager(resultSet, managerId);

            return new Employee(id, fullName, position, hiredate, salary, manager);
        } catch (SQLException e) {
            LOGGER.error("Something went wrong at getEmployee!!!", e);
            throw new RuntimeException(e);
        }
    }

    private Employee getManager(ResultSet resultSet, int managerId) {
        Employee manager = null;
        try {
            int rowForBack = resultSet.getRow();
            resultSet.beforeFirst();
            while (resultSet.next()) {
                if (resultSet.getInt(ID) == managerId) {
                    manager = getEmployee(resultSet);
                    break;
                }
            }
            resultSet.absolute(rowForBack);
        } catch (SQLException e) {
            LOGGER.error("Something went wrong at getManager!!!", e);
            throw new RuntimeException(e);
        }
        return manager;
    }

    private LocalDate getHiredate(ResultSet resultSet) throws SQLException {
        return resultSet.getDate(HIREDATE).toLocalDate();
    }

    private FullName getFullName(ResultSet resultSet) throws SQLException {
        String firstName = resultSet.getString(FIRSTNAME);
        String lastName = resultSet.getString(LASTNAME);
        String middleName = resultSet.getString(MIDDLENAME);
        return new FullName(firstName, lastName, middleName);
    }

    private Position getPosition(ResultSet resultSet) throws SQLException {
        String rawPosition = resultSet.getString(POSITION);
        return Position.valueOf(rawPosition.toUpperCase());
    }
}