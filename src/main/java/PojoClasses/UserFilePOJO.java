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
    private String account_id;
    private String premise_id;
    private String user_segment;
    private String file_abs_path;
}
