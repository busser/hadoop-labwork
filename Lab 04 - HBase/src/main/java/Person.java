import java.util.List;

/**
 * Created by Arthur on 11/4/2016.
 */
public class Person {

    private String firstName;
    private String lastName;
    private String birthDate;
    private String city;
    private String email;
    private String bff;
    private List<String> otherFriends;

    public Person(String firstName, String lastName, String birthDate, String city, String email, String bff, List<String> otherFriends) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthDate = birthDate;
        this.city = city;
        this.email = email;
        this.bff = bff;
        this.otherFriends = otherFriends;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getBff() {
        return bff;
    }

    public void setBff(String bff) {
        this.bff = bff;
    }

    public List<String> getOtherFriends() {
        return otherFriends;
    }

    public void setOtherFriends(List<String> otherFriends) {
        this.otherFriends = otherFriends;
    }
}
