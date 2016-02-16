/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

object T2dmPhenotype {
  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc for phenotype like example code below
      * as while testing your code we expect your function have no side effect.
      * i.e. Do NOT read from file or write file
      *
      * You don't need to follow the example placeholder codes below exactly, once you return the same type of return.
      */

    val sc = medication.sparkContext

    /** Hard code the criteria */
    val type1_dm_dx = Set("code1", "250.03")
    val type1_dm_med = Set("med1", "insulin nph")

    /** Find CASE Patients */
    val casePatients = sc.parallelize(Seq(("casePatient-one", 1), ("casePatient-two", 1), ("casePatient-three", 1)))

    /** Find CONTROL Patients */
    val controlPatients = sc.parallelize(Seq(("controlPatients-one", 2), ("controlPatients-two", 2), ("controlPatients-three", 2)))

    /** Find OTHER Patients */
    val others = sc.parallelize(Seq(("others-one", 3), ("others-two", 3), ("others-three", 3)))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
}
