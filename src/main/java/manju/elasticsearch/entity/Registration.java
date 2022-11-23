package manju.elasticsearch.entity;

import java.sql.Date;

public class Registration {


    private Integer id;
    private String first_name;
    private String last_name;
    private Integer age;

 
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }
    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }
    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
   
    @Override
    public String toString() {
        return String.format("Registration{id='%s', first_name='%s', last_name='%s',age='%s'}", 
            id, first_name,last_name,age);
    }
}
