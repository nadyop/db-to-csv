package id.nad.batch.process;

import id.nad.batch.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class InputProcess implements ItemProcessor<DataModel, DataModel>{

    private static final Logger LOGGER = LoggerFactory.getLogger(InputProcess.class);

    @Override
    public DataModel process(DataModel dataModel) {
        LOGGER.info("Proses input : {}", dataModel);

        DataModel output = new DataModel();
        output.setId(dataModel.getId());
        output.setName(dataModel.getName());

        LOGGER.info("Input yang telah selesai : {}", output);
        return output;
    }
}
