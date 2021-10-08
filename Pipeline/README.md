# Tick Spatio-temporal dynamics modeler

The Tick spatiotemporal dynamics modeler uses a facade pattern to obscure the complexity of the implementation while provides a transparent interface for the users. 
The facade class named TickPipeline uses several underlying classes for implementing the pipeline namely, ExternalDataHandling, ModelingSpatioTemporal.

## ExternalDataHandling

This class is intended for data retrieval, preprocessing, and providing an interface for external applications to read the resulting data. 

Results should be returned in an web API (e.g. using flask).

## TickModeler

This class is the main implementation of the modeling stage. The model generator class is designed in principle for implementing a model selection pre-stage, trainning, and evaluation of results, and an independent method for forward forecasting. This class is also designed to allow an extended implementation of different models (initially as an interface for processing input and outputs from an external process).