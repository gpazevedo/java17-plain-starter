package br.com.gruposbf.burnwood;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.logging.Level;
import java.util.logging.Logger;

public class BeamPrint <T> extends PTransform<PCollection<T>, PDone> {
    static Logger log = Logger.getLogger(BeamPrint.class.getName());

    @Override
    public PDone expand(PCollection<T> input) {
        input.apply(ParDo.of(new BeamPrint.LogResultsFn<>()));
        return PDone.in(input.getPipeline());
    }
    private static class LogResultsFn<T> extends DoFn<T, Void> {
        @ProcessElement
        public void process(@Element T elem) {
            log.log(Level.INFO, elem.toString());
        }
    }

}
