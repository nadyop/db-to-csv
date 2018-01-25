package id.nad.batch.config.batch;

import id.nad.batch.config.BatchListener;
import id.nad.batch.config.HeaderWriter;
import id.nad.batch.model.DataModel;
import id.nad.batch.process.InputProcess;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.sql.ResultSet;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchConfig.class);
    private static final String PROPERTY_CSV_EXPORT_FILE_HEADER = "database.to.csv.job.export.file.header";
    private static final String PROPERTY_CSV_EXPORT_FILE_PATH = "database.to.csv.job.export.file.path";

    @Bean
    public ItemReader<DataModel> reader(@Qualifier("dataSource") DataSource dataSource) {
        JdbcCursorItemReader<DataModel> reader = new JdbcCursorItemReader<DataModel>();
        reader.setSql("select * from data");
        reader.setDataSource(dataSource);
        reader.setRowMapper(
                (ResultSet rs, int rowNum) -> {
                    LOGGER.info("RowMapper rs: {}", rs);
                    if (!(rs.isAfterLast()) && !(rs.isBeforeFirst())){
                        DataModel input = new DataModel();
                        input.setId(rs.getInt("id"));
                        input.setName(rs.getString("name"));

                        LOGGER.info("RowMapper record : {}", input);
                        return input;
                    }else{
                        LOGGER.info("Returning null of Row Mapper");
                        return null;
                    }
                }
        );
        return reader;
    }

    @Bean
    public ItemProcessor<DataModel, DataModel> processor(){
        return new InputProcess();
    }

    @Bean
    public ItemPreparedStatementSetter<DataModel> setter(){
        return (item, ps) -> {
            ps.setInt(1, item.getId());
            ps.setString(2, item.getName());
        };
    }

    @Bean
    public ItemWriter<DataModel> writer(Environment environment){
        FlatFileItemWriter<DataModel> fileItemWriter = new FlatFileItemWriter<>();
        String exportFileHeader = environment.getRequiredProperty(PROPERTY_CSV_EXPORT_FILE_HEADER);
        HeaderWriter headerWriter = new HeaderWriter(exportFileHeader);
        fileItemWriter.setHeaderCallback(headerWriter);

        String exportFilePath = environment.getRequiredProperty(PROPERTY_CSV_EXPORT_FILE_PATH);
        fileItemWriter.setResource(new FileSystemResource(exportFilePath));

        LineAggregator<DataModel> lineAggregator = createDataLineAggregator();
        fileItemWriter.setLineAggregator(lineAggregator);

        return fileItemWriter;
    }

    private FieldExtractor<DataModel> createDataFieldExtractor(){
        BeanWrapperFieldExtractor<DataModel> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] {"id","name"});
        return extractor;
    }

    private LineAggregator<DataModel> createDataLineAggregator() {
        DelimitedLineAggregator<DataModel> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");

        FieldExtractor<DataModel> fieldExtractor = createDataFieldExtractor();
        lineAggregator.setFieldExtractor(fieldExtractor);

        return lineAggregator;
    }

    @Bean
    Step stepDbToCsv(ItemReader<DataModel> readerDbCsv,
                     ItemProcessor<DataModel, DataModel> processDbCsv,
                     ItemWriter<DataModel> writerDbCsv,
                     StepBuilderFactory stepBuilderFactory){
        return stepBuilderFactory.get("step database to csv")
                .<DataModel, DataModel>chunk(100)
                .reader(readerDbCsv)
                .processor(processDbCsv)
                .writer(writerDbCsv)
                .build();
    }

    @Bean
    Job jobDbToCsv(JobBuilderFactory jobBuilderFactory,
                             @Qualifier("stepDbToCsv") Step csvStudentStep,
                             BatchListener batchListener) {
        return jobBuilderFactory.get("databaseToCsvFileJob")
                .listener(batchListener)
                .incrementer(new RunIdIncrementer())
                .flow(csvStudentStep)
                .end()
                .build();
    }
}
