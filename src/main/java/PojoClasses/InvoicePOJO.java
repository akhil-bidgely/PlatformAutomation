package PojoClasses;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class InvoicePOJO {
    private String account_id;
    private String premise_id;
    private String data_Stream_Id;
    private String fuel_type;
    private String mr_cycle_code;
    private String bc_start_date;
    private String bc_end_date;
    private String charge_type;
    private String kWh_Consumption;
    private String dollar_cost;
    private String meter_type;
}
