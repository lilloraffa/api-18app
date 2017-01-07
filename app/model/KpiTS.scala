package model

import java.time.LocalDate

case class KpiTS (  
    date: List[LocalDate],
    kpi: List[Double]
)