package PojoClasses;

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

    private String email = "akhil+500401002@bidgely.com";
    private String first_name = "Akhil";
    private String last_name = "Sharma";
    private String address_1 = "221B Baker Street";
    private String address_2;
    private String city = "Hackney";
    private String state = "England";
    private String postal_code = "208014";
//    private String file_abs_path;
}
