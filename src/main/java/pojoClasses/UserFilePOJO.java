package pojoClasses;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class UserFilePOJO {
    private String customer_id;
    private String partner_user_id;
    private String premise_id;

    private String email;
    private String first_name;
    private String last_name;
    private String address_1;
    private String address_2;
    private String city;
    private String state;
    private String postal_code;

    private String uuid;
}
