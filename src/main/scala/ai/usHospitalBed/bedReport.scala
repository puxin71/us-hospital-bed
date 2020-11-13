package ai.usHospitalBed

final case class bedReport(
  objectId: Long,
  adultIcuBed: Long,
  pediIcuBed: Long,
  averageVentilatorUsage: Double,
  bedUtilizationRate: Double,
  city: String,
  country: String,
  state: String,
  hospitalName: String,
  hospitalType: String
)