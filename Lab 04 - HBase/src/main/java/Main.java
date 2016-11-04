import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {

        SocialNetwork sn = null;

        try {
            sn = new SocialNetwork();
            System.out.println("Connection to HBase successful.");
        } catch (Exception e) {
            System.out.println("Connection to HBase failed. Closing...");
            System.exit(1);
        }

        System.out.println("Welcome to our HBase REPL.");

        sn.getAllRecord("flefloch");

        System.out.println("Possible commands:");
        System.out.println("add person firstname=<string>\n"
                         + "           bff=<string>\n"
                         + "           [lastname=<string>]\n"
                         + "           [birthdate=<string>]\n"
                         + "           [city=<string>]\n"
                         + "           [email=<string>]\n"
                         + "           [otherfriends=<string>,<string>,<string>]"
        );
        System.out.println("exit");

        boolean exit = false;
        Scanner inputScanner = new Scanner(System.in);

        while (!exit) {

            System.out.print("$: ");
            String input = inputScanner.nextLine();
            List<String> inputList = Arrays.asList(input.split(" "));

            if (inputList.size() > 0 && inputList.get(0).length() > 0) {

                if (inputList.get(0).equals("add")) {

                    if (inputList.size() > 1 && inputList.get(1).equals("person")) {

                        Map<String, String> parameters = new HashMap<String, String>();
                        for (int i = 2; i < inputList.size(); i++) {
                            String[] splitParameters = inputList.get(i).split("=", 2);
                            parameters.put(inputList.get(i).split("=", 2)[0], inputList.get(i).split("=", 2)[1]);
                        }

                        Set<String> validKeys = new HashSet<String>();
                        validKeys.add("firstname");
                        validKeys.add("lastname");
                        validKeys.add("birthdate");
                        validKeys.add("email");
                        validKeys.add("city");
                        validKeys.add("bff");
                        validKeys.add("otherfriends");

                        Set<String> necessaryKeys = new HashSet<String>();
                        necessaryKeys.add("firstname");
                        necessaryKeys.add("bff");

                        parameters.keySet().retainAll(validKeys);
                        parameters.values().removeAll(Collections.singleton(null));
                        if (!parameters.keySet().containsAll(necessaryKeys)) {

                            System.out.println("The following parameters must be included: " + StringUtils.join(necessaryKeys, ", "));

                        } else {

                            String otherFriends = parameters.get("otherFriends");
                            Person person = new Person(parameters.get("firstname"), parameters.get("lastname"), parameters.get("birthdate"),
                                    parameters.get("email"), parameters.get("city"), parameters.get("bff"),
                                    otherFriends == null ? null : Arrays.asList(otherFriends.split(",")));

                            try {
                                sn.addPerson(person);
                            } catch (Exception e) {
                                System.out.println("There was a problem when adding person '" + parameters.get("firstname") + "'.");
                                e.printStackTrace();
                            }

                        }

                    } else {

                        System.out.println("Unknown use of the 'add' command.");

                    }

                } else if (inputList.get(0).equals("exit")) {

                    exit = true;

                } else {

                    System.out.println("Unknown command.");

                }

            }

        }

        inputScanner.close();

    }

}
