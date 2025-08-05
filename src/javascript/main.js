import cep from 'cep-promise';
import fs from 'fs';
import path from 'path';
import Readline from 'readline';
import os from 'os';



class ScrapAPIs {
    /**
     * Constructor da classe ScrapAPIs
     * @param {number} [time_sleep=2000] - Tempo de espera entre requisições em milissegundos.
     * @param {string[]} [providers=['brasilapi','viacep','widenet','correios','correios-alt']] - Lista de provedores a serem usados.
     * @throws {Error} - Se o tempo de espera for invalido ou a lista de provedores for vazia ou invalida.
     */
    constructor(time_sleep = 2000, providers = ['brasilapi','viacep','widenet','correios','correios-alt']) {
        this.time_sleep = time_sleep;
        this.providers = providers;

        if (this.time_sleep <= 0 || isNaN(this.time_sleep) || !Number.isInteger(this.time_sleep)) {
            console.log('Invalid timeout. Must be a positive integer.');
            return;
        } else {
            console.log(`Tempo de espera (timesleep):\n${this.time_sleep / 1000} segundos`);
        }

        if (this.providers.length <= 0 || 
            !Array.isArray(this.providers) || 
            this.providers.some(provider => typeof provider !== 'string') || 
            this.providers.some(provider => provider.trim() === '') ||
            this.providers.some(provider => !['brasilapi','viacep','widenet','correios','correios-alt'].includes(provider))
        ) {
            console.log('\
                \nInvalid providers.\
                \nMust be an array with at least one provider. \
                \nOptions: "brasilapi", "viacep", "widenet", "correios", "correios-alt"\
                \n');
            return;
        } else {
            const providers_string = JSON.stringify(this.providers);
            console.log(`\nAPIs escolhidas:\n${providers_string}\n`);
        }


        /**
        * Faz uma pergunta ao usuario e retorna uma promessa com a resposta
        * @param {String} question - Pergunta a ser feita ao usuario
        * @returns {Promise<String>} - Promessa com a resposta do usuario
        */
        this.input = async function (question) {
            const rl = Readline.createInterface({
                input: process.stdin,
                output: process.stdout
            });
            return new Promise(resolve => {
                rl.question(question, answer => {
                rl.close();
                resolve(answer);
                });
            });
        }

    }



    /**
     * Trata arquivos json na pasta datasets e retorna a resposta escolhida pelo usuario
     * @returns {Promise<String>} - Promessa com o caminho do arquivo escolhido
     */
    async choose_file() {

        const all_files_at_path = fs.readdirSync(path.resolve('src','datasets'));
        const editions_json_files = all_files_at_path.filter(each_file => each_file.endsWith('.json'));

        console.log('Arquivos encontrados:');
        console.log(editions_json_files);

        const selected_file = await this.input("\nArquivo a carregar: ");
        const path_to_file = path.resolve('src','datasets', selected_file);

        if (!fs.existsSync(path_to_file)) {
            console.log(`File "${path_to_file}" not found.`);
        return;
        }

        console.log(`Arquivo selecionado:\n"${path.resolve('src','datasets', path_to_file)}"`);
        
        return path_to_file;
    }



    /**
     * Lê o arquivo JSON especificado e realiza requests para obter informa es
     * de CEPs e salva o resultado em um novo arquivo JSON
     * @param {String} path_to_file - Caminho do arquivo JSON a ser lido
     * @returns {Promise<void>}
     */
    async requests(path_to_file) {
        fs.readFile(path_to_file, 'utf8', async (err, data) => {
            if (err) {
                console.error(err);
                return;
            }

            const json_data = JSON.parse(data);
            let result_json = {};

            for (const item of json_data) {
                if (!item.cep) continue;

                try {
                    const result = await cep(item.cep, { providers: this.providers });
                    console.log(result)
                    result_json[item.cep] = result;
                    console.log(`CEP ${item.cep} → OK`);
                    await new Promise(r => setTimeout(r, this.time_sleep));
                } catch (e) {
                    console.error(`Erro no CEP ${item.cep}:`, e.message);
                }
            }

            const output_path = path.resolve('src', 'datasets', 'resultado.json');
            fs.writeFileSync(output_path, JSON.stringify(result_json, null, 2));
            console.log(`Resultado salvo em ${output_path}`);
        });
    }
}

async function main() {
    const scrap = new ScrapAPIs(5000, ['brasilapi', 'viacep']);
    const path_to_file = await scrap.choose_file();
    if (path_to_file) {
        await scrap.requests(path_to_file);
    }
}

main();
