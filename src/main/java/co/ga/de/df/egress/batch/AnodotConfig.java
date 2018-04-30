package co.ga.de.df.egress.batch;

import com.anodot.metrics.Anodot;
import com.anodot.metrics.AnodotMetricRegistry;
import com.anodot.metrics.AnodotReporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AnodotConfig {

    @Value("${anodot.api-token}")
    protected String anodotApiToken;

    @Bean(name = "anodot")
    protected Anodot anodot(){
       return  new Anodot("https://api.anodot.com/api/v1/metrics", anodotApiToken);

    }
}
