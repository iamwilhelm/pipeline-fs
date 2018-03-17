# What is this?

An experiment of how data could be version controlled and signed by a pipeline. It 
implements the basic parts of a git file system, using data structures from IPFS.

We see if we can sign it with pipeline stages, and whether we can execute it
in order using a DAG.

# Conclusion

For tasks that are run sequentially and in batches, you'd need to use and 
calculate a DAG. However, if pipeline is streaming, you don't actually need
any of that. Given that a streaming kappa architecture can encompass batch
processing work, we can just go with streaming.

# Setup

## Developer setup

```
pipenv install
pipenv shell
```

## Demo

Just run the `wordcount_pipeline.py` for a demo.

It puts together the pipeline and runs them synchronously. It calculates the 
DAG and runs the stages in order.


