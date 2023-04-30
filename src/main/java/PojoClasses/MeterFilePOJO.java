package PojoClasses;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class MeterFilePOJO {
    private String customer_id;
    private String partner_user_id;
    private String premise_id;
    private String data_stream_id;
    private String service_type;
    private String data_stream_type;
    private String file_abs_path;
    private String meter_type;
}
