import querier
import parser


if __name__ == '__main__':
    wf, task, lp = querier.get_all_entities()
    parser.generate_directory(wf, task, lp)




