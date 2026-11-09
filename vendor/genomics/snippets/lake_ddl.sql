CREATE SCHEMA IF NOT EXISTS lake.genomics;
CREATE TABLE IF NOT EXISTS lake.genomics.genotypes (
  chrom varchar, pos bigint, ref varchar, alt varchar, filter varchar, af real,
  sample_id varchar, gt tinyint, gq smallint, dp smallint)
WITH (external_location = 's3://rad-lab-enclave-cohort/parquet/genotypes/', format = 'PARQUET');
CREATE TABLE IF NOT EXISTS lake.genomics.variants (
  chrom varchar, pos bigint, ref varchar, alt varchar, qual real, filter varchar, ac integer, an integer, af real,
  af_afr real, af_amr real, af_eas real, af_eur real, af_sas real, effect varchar, impact varchar, gene varchar,
  coding boolean, cadd_phred real, phylop real)
WITH (external_location = 's3://rad-lab-enclave-cohort/parquet/variants/', format = 'PARQUET');
CREATE TABLE IF NOT EXISTS lake.genomics.samples (
  sample_id varchar, family_id varchar, father_id varchar, mother_id varchar, sex tinyint,
  population varchar, superpopulation varchar)
WITH (external_location = 's3://rad-lab-enclave-cohort/parquet/samples/', format = 'PARQUET');
