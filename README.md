# Distributed computing final project
Krishna Kalakkad, Laura McGann, Ethan Waite, Ethan Zimbelman

## The Uber & Lyft cab prices dataset

We are using the [Uber & Lyft cab prices dataset](https://www.kaggle.com/ravi72munde/uber-lyft-cab-prices).

### For development

A downloaded copy of this data can be found in `input/`. To automatically sync
to the HDFS, run `./upload.sh` from the `input/` directory.

**Note**: `/user/USERNAME/input` must not exist on the HDFS before running this!

## Repo structure

All Scala/Spark code can be found in `source/`, with actual source code in the
`src/` directory.

To quickly compile and run the program, run `./run.sh` from the `source/`
directory. You must have the dataset uploaded to the HDFS prior to running the
program.

## Deliverables

 - [presentation slides](https://docs.google.com/presentation/d/1ygSLH_IqH7dF4e9iZE1dgqhRUACl5WMx_TiYJVA49vI/edit?usp=sharing)
 - [final report](https://docs.google.com/document/d/1bvpbT2DmOuvZ0I8sFsQFajMB8Kf4MUByAWQ7x-kZLnk/edit?usp=sharing)
