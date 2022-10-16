import numpy as np
import pandas as pd
import os
import glob
import json

# Pandas display options
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option('display.width', 1000)

# Grabs the legend for group 1 and inverts the dictionary
with open('annotations/Group1/annotations-legend.json', 'r') as legend:
    legend1 = legend.read()
    legend1 = json.loads(legend1)
    legend1 = {v: k for k, v in legend1.items()}

# Grabs the legend for group 2 and inverts the dictionary
with open('annotations/Group2/annotations-legend.json', 'r') as legend:
    legend2 = legend.read()
    legend2 = json.loads(legend2)
    legend2 = {v: k for k, v in legend2.items()}

# Initializes dataframe with all Columns
grid = pd.DataFrame(
    columns=['RootCode', 'PentaCode', 'NumOfEvents', 'MultiAction', 'Reciprocal', 'Source1', 'Target1', 'Source2',
             'Target2', 'Source3', 'Target3', 'Source4', 'Target4', 'Source5', 'Target5', 'Source6', 'Target6',
             'Source7', 'Target7', 'notConfident_Action', 'notConfident_SrcTgt'])


# grabs entity values from json file
def entityGrabber(row, jsonText, id, index):
    try:
        for i in range(14):
            if jsonText['entities'][i]['classId'] == id:
                listValue = i
                break
        row = row.append(pd.Series([{jsonText['entities'][listValue]['offsets'][0]['text']}], index=[index]))

    except KeyError:
        pass
    except IndexError:
        pass
    return row


# Grabs meta values from the json file
def metaGrabber(row, jsonText, id, index):
    try:
        row = row.append(pd.Series([{jsonText['metas'][id]['value']}], index=[index]))
    except KeyError:
        pass
    return row


# Goes through every value in a specified folder
def checkFolder(folder, df, legend):
    for filename in glob.glob(os.path.join(folder, '*.json')):
        with open(filename, 'r') as f:
            text = f.read()
            jsonText = json.loads(text)
            row = pd.Series([])
            # tagtog_id
            row = row.append(pd.Series([filename[-13:-9]], index=['tagtog_id']))
            # Root Code
            row = metaGrabber(row, jsonText, legend['RootCode'], 'RootCode')
            # Penta Code
            row = metaGrabber(row, jsonText, legend['PentaCode'], 'PentaCode')
            # Reciprocal
            row = metaGrabber(row, jsonText, legend['Reciprocal'], 'Reciprocal')
            # Multi-Action
            row = metaGrabber(row, jsonText, legend['MultiAction'], 'MultiAction')
            # NumOfEvents
            row = metaGrabber(row, jsonText, legend['NumOfEvents'], 'NumOfEvents')
            # notConfident_SrcTgt
            row = metaGrabber(row, jsonText, legend['notConfident_SrcTgt'], 'notConfident_SrcTgt')
            # notConfident_Action
            row = metaGrabber(row, jsonText, legend['notConfident_Action'], 'notConfident_Action')
            # Source1
            row = entityGrabber(row, jsonText, legend['Source1'], 'Source1')
            # Source2
            row = entityGrabber(row, jsonText, legend['Source2'], 'Source2')
            # Source3
            row = entityGrabber(row, jsonText, legend['Source3'], 'Source3')
            # Source4
            row = entityGrabber(row, jsonText, legend['Source4'], 'Source4')
            # Source5
            row = entityGrabber(row, jsonText, legend['Source5'], 'Source5')
            # Source6
            row = entityGrabber(row, jsonText, legend['Source6'], 'Source6')
            # Source7
            row = entityGrabber(row, jsonText, legend['Source7'], 'Source7')
            # Target1
            row = entityGrabber(row, jsonText, legend['Target1'], 'Target1')
            # Target2
            row = entityGrabber(row, jsonText, legend['Target2'], 'Target2')
            # Target3
            row = entityGrabber(row, jsonText, legend['Target3'], 'Target3')
            # Target4
            row = entityGrabber(row, jsonText, legend['Target4'], 'Target4')
            # Target5
            row = entityGrabber(row, jsonText, legend['Target5'], 'Target5')
            # Target6
            row = entityGrabber(row, jsonText, legend['Target6'], 'Target6')
            # Target7
            row = entityGrabber(row, jsonText, legend['Target7'], 'Target7')

            df = df.append(row, ignore_index=True)
    df = df.set_index('tagtog_id')
    return df


# Makes a dataframe for annotator 3
def annontator3(xlsx):
    df = pd.read_excel(xlsx)
    pentaCodeCol = pd.Series([])
    # df = df.drop(['original_id'])
    for i in range(len(df['ud_rootCode'])):
        if df['ud_rootCode'][i] == 1 or df['ud_rootCode'][i] == 2:
            pentaCode = 0
        elif df['ud_rootCode'][i] == 3 or df['ud_rootCode'][i] == 4 or df['ud_rootCode'][i] == 5:
            pentaCode = 1
        elif df['ud_rootCode'][i] == 6 or df['ud_rootCode'][i] == 7 or df['ud_rootCode'][i] == 8:
            pentaCode = 2
        elif df['ud_rootCode'][i] == 9 or df['ud_rootCode'][i] == 10 or df['ud_rootCode'][i] == 11 or df['ud_rootCode'][
            i] == 12 or df['ud_rootCode'][i] == 13 or df['ud_rootCode'][i] == 16:
            pentaCode = 3
        elif df['ud_rootCode'][i] == 14 or df['ud_rootCode'][i] == 15 or df['ud_rootCode'][i] == 17 or \
                df['ud_rootCode'][i] == 18 or df['ud_rootCode'][i] == 19 or df['ud_rootCode'][i] == 20:
            pentaCode = 4
        pentaCodeCol = pentaCodeCol.append(pd.Series([pentaCode]))

    df = df.assign(ud_pentaCodes=pentaCodeCol.values)
    df = df.set_index('tagtog_id')
    return df


grid1 = checkFolder('annotations/Group2/annotator1', grid, legend2)
grid2 = checkFolder('annotations/Group2/annotator2', grid, legend2)
annotate2_3 = annontator3('annotations/Group2/annotator3/all_sentences.xlsx')


# Distribution of MultiAction, Reciprocal, NumOfEvents, RootCode and PentaCode (1a)
def distribution(grid):
    RootDist = grid['RootCode'].value_counts(normalize=True)
    PentaDist = grid['PentaCode'].value_counts(normalize=True)
    NumOfEventsDist = grid['NumOfEvents'].value_counts(normalize=True)
    ReciprocalDist = grid['Reciprocal'].value_counts(normalize=True)
    MultiActionDist = grid['MultiAction'].value_counts(normalize=True)
    return {'RootDist': RootDist, 'PentaDist': PentaDist, 'NumOfEventsDist': NumOfEventsDist,
            'ReciprocalDist': ReciprocalDist, 'MultiActionDist': MultiActionDist}


grid1Dist = distribution(grid1)
grid2Dist = distribution(grid2)


# print(grid1Dist)
# print(grid2Dist)


# Proportion and absolute value of entries with same MultiAction Flag (1b)(Ask if no input should count as false)
def matchingMultiAction(grid1, grid2):
    sameMultiAction = pd.Series(data=[])
    for i in range(len(grid1['MultiAction'])):
        grid1Value = grid1['MultiAction'][i]
        gridId = grid1['MultiAction'].index[i]
        try:
            grid2Value = grid2['MultiAction'][gridId]
        except KeyError:
            continue
        if grid1Value == grid2Value:
            sameMultiAction = sameMultiAction.append(pd.Series([True], index=[gridId]))
        else:
            sameMultiAction = sameMultiAction.append(pd.Series([False], index=[gridId]))

    sameMultiActionProp = sameMultiAction.value_counts(normalize=True)
    sameMultiActionValue = sameMultiAction.value_counts()
    return {'sameMultiActionProp': sameMultiActionProp, 'sameMultiActionValue': sameMultiActionValue}


sameMultiAction = matchingMultiAction(grid1, grid2)


# print(sameMultiAction['sameMultiActionProp'])
# print(sameMultiAction['sameMultiActionValue'])


# Proportion and absolute value of entries with the same Reciprocal Flag (1c)
def matchingReciprocal(grid1, grid2):
    sameReciprocal = pd.Series(data=[])
    for i in range(len(grid1['Reciprocal'])):
        grid1Value = grid1['Reciprocal'][i]
        gridId = grid1['Reciprocal'].index[i]
        try:
            grid2Value = grid2['Reciprocal'][gridId]
        except KeyError:
            continue
        if grid1Value == grid2Value:
            sameReciprocal = sameReciprocal.append(pd.Series([True], index=[gridId]))
        else:
            sameReciprocal = sameReciprocal.append(pd.Series([False], index=[gridId]))

    sameReciprocalProp = sameReciprocal.value_counts(normalize=True)
    sameReciprocalValue = sameReciprocal.value_counts()
    return {'sameReciprocalProp': sameReciprocalProp, 'sameReciprocalValue': sameReciprocalValue}


sameReciprocal = matchingReciprocal(grid1, grid2)


# print(sameReciprocal['sameReciprocalProp'])
# print(sameReciprocal['sameReciprocalValue'])


# Proportion and absolute value of entries with same NumOfEvents (1d)
def matchingNumOfEvents(grid1, grid2):
    sameNumOfEvents = pd.Series(data=[])
    for i in range(len(grid1['NumOfEvents'])):
        grid1Value = grid1['NumOfEvents'][i]
        gridId = grid1['NumOfEvents'].index[i]
        try:
            grid2Value = grid2['NumOfEvents'][gridId]
        except KeyError:
            continue
        if grid1Value == grid2Value:
            sameNumOfEvents = sameNumOfEvents.append(pd.Series([True], index=[gridId]))
        else:
            sameNumOfEvents = sameNumOfEvents.append(pd.Series([False], index=[gridId]))

    sameNumOfEventsProp = sameNumOfEvents.value_counts(normalize=True)
    sameNumOfEventsValue = sameNumOfEvents.value_counts()
    return {'sameNumOfEventsProp': sameNumOfEventsProp, 'sameNumOfEventsValue': sameNumOfEventsValue}


sameNumOfEvents = matchingNumOfEvents(grid1, grid2)


# print(sameNumOfEvents['sameNumOfEventsProp'])
# print(sameNumOfEvents['sameNumOfEventsValue'])


# Proportion and absolute value of entries with the same RootCode (1e)
def matchingRootCode(grid1, grid2):
    sameRootCode = pd.Series(data=[])
    for i in range(len(grid1['RootCode'])):
        grid1Value = grid1['RootCode'][i]
        gridId = grid1['RootCode'].index[i]
        try:
            grid2Value = grid2['RootCode'][gridId]
        except KeyError:
            continue
        if grid1Value == grid2Value:
            sameRootCode = sameRootCode.append(pd.Series([True], index=[gridId]))
        else:
            sameRootCode = sameRootCode.append(pd.Series([False], index=[gridId]))

    sameRootCodeProp = sameRootCode.value_counts(normalize=True)
    sameRootCodeValue = sameRootCode.value_counts()
    return {'sameRootCodeProp': sameRootCodeProp, 'sameRootCodeValue': sameRootCodeValue}


sameRootCode = matchingRootCode(grid1, grid2)


# print(sameRootCode['sameRootCodeProp'])
# print(sameRootCode['sameRootCodeValue'])


# Proportion and absolute value of entries with the same PentaCode (1f)
def matchingPentaCode(grid1, grid2):
    samePentaCode = pd.Series(data=[])
    for i in range(len(grid1['PentaCode'])):
        grid1Value = grid1['PentaCode'][i]
        gridId = grid1['PentaCode'].index[i]
        try:
            grid2Value = grid2['PentaCode'][gridId]
        except KeyError:
            continue
        if grid1Value == grid2Value:
            samePentaCode = samePentaCode.append(pd.Series([True], index=[gridId]))
        else:
            samePentaCode = samePentaCode.append(pd.Series([False], index=[gridId]))
    samePentaCodeProp = samePentaCode.value_counts(normalize=True)
    samePentaCodeValue = samePentaCode.value_counts()
    return {'samePentaCodeProp': samePentaCodeProp, 'samePentaCodeValue': samePentaCodeValue}


samePentaCode = matchingPentaCode(grid1, grid2)


# print(samePentaCode['samePentaCodeProp'])
# print(samePentaCode['samePentaCodeValue'])


# Proportion and absolute value of entries with the same rootCode filtering out
# when either annotator checks the multiAction flag (1g)
def rootCodeWithoutMultiAction(grid1, grid2):
    sameRootCode = pd.Series(data=[])
    for i in range(len(grid1['RootCode'])):
        grid1Value = grid1['RootCode'][i]
        gridId = grid1['RootCode'].index[i]
        try:
            grid2Value = grid2['RootCode'][gridId]
        except KeyError:
            continue
        if grid1['MultiAction'][gridId] == {True} or grid2['MultiAction'][gridId] == {True}:
            continue
        if grid1Value == grid2Value:
            sameRootCode = sameRootCode.append(pd.Series([True], index=[gridId]))
        else:
            sameRootCode = sameRootCode.append(pd.Series([False], index=[gridId]))

    sameRootCodeProp = sameRootCode.value_counts(normalize=True)
    sameRootCodeValue = sameRootCode.value_counts()
    return {'sameRootCodeProp': sameRootCodeProp, 'sameRootCodeValue': sameRootCodeValue}


rootCodeWithoutMultiAction = rootCodeWithoutMultiAction(grid1, grid2)


# print(rootCodeWithoutMultiAction['sameRootCodeProp'])
# print(rootCodeWithoutMultiAction['sameRootCodeValue'])


# Proportion and absolute value of entries with the same pentaCode filtering out
# when either annotator checks the multiAction flag (1h)
def pentaCodeWithoutMultiAction(grid1, grid2):
    samePentaCode = pd.Series(data=[])
    for i in range(len(grid1['PentaCode'])):
        grid1Value = grid1['PentaCode'][i]
        gridId = grid1['PentaCode'].index[i]
        try:
            grid2Value = grid2['PentaCode'][gridId]
        except KeyError:
            continue
        if grid1['MultiAction'][gridId] == {True} or grid2['MultiAction'][gridId] == {True}:
            continue
        if grid1Value == grid2Value:
            samePentaCode = samePentaCode.append(pd.Series([True], index=[gridId]))
        else:
            samePentaCode = samePentaCode.append(pd.Series([False], index=[gridId]))
    samePentaCodeProp = samePentaCode.value_counts(normalize=True)
    samePentaCodeValue = samePentaCode.value_counts()
    return {'samePentaCodeProp': samePentaCodeProp, 'samePentaCodeValue': samePentaCodeValue}


pentaCodeWithoutMultiAction = pentaCodeWithoutMultiAction(grid1, grid2)


# print(pentaCodeWithoutMultiAction['samePentaCodeProp'])
# print(pentaCodeWithoutMultiAction['samePentaCodeValue'])


# Proportion and absolute value of matching RootCodes across all annotators (1i)
def matchingRootCodeAll(grid1, grid2, annotate3):
    sameRootCode = pd.Series(data=[])
    for i in range(len(grid1['RootCode'])):
        grid1Value = grid1['RootCode'][i]
        gridId = grid1['RootCode'].index[i]
        try:
            grid2Value = grid2['RootCode'][gridId]
            grid3Value = {str(annotate3['ud_rootCode'][int(gridId)])}
        except KeyError:
            continue
        if grid1Value == grid2Value == grid3Value:
            sameRootCode = sameRootCode.append(pd.Series([True], index=[gridId]))
        else:
            sameRootCode = sameRootCode.append(pd.Series([False], index=[gridId]))

    sameRootCodeProp = sameRootCode.value_counts(normalize=True)
    sameRootCodeValue = sameRootCode.value_counts()
    return {'sameRootCodeProp': sameRootCodeProp, 'sameRootCodeValue': sameRootCodeValue}


matchingRootCodeAll = matchingRootCodeAll(grid1, grid2, annotate2_3)


# print(matchingRootCodeAll['sameRootCodeProp'])
# print(matchingRootCodeAll['sameRootCodeValue'])


# Proportion and absolute value of matching PentaCodes across all annotators (1j)
def matchingPentaCodeAll(grid1, grid2, annotate3):
    samePentaCode = pd.Series(data=[])
    for i in range(len(grid1['PentaCode'])):
        grid1Value = grid1['PentaCode'][i]
        gridId = grid1['PentaCode'].index[i]
        try:
            grid2Value = grid2['PentaCode'][gridId]
            grid3Value = {str(annotate3['ud_pentaCodes'][int(gridId)])}
        except KeyError:
            continue

        if grid1Value == grid2Value == grid3Value:
            samePentaCode = samePentaCode.append(pd.Series([True], index=[gridId]))
        else:
            samePentaCode = samePentaCode.append(pd.Series([False], index=[gridId]))
    samePentaCodeProp = samePentaCode.value_counts(normalize=True)
    samePentaCodeValue = samePentaCode.value_counts()
    return {'samePentaCodeProp': samePentaCodeProp, 'samePentaCodeValue': samePentaCodeValue}


matchingPentaCodeAll = matchingPentaCodeAll(grid1, grid2, annotate2_3)


# print(matchingPentaCodeAll['samePentaCodeProp'])
# print(matchingPentaCodeAll['samePentaCodeValue'])


# Proportion and absolute value of matching RootCodes across all annotators no MultiAction(1k)
def allRootCodeNoMulti(grid1, grid2, annotate3):
    sameRootCode = pd.Series(data=[])
    for i in range(len(grid1['RootCode'])):
        grid1Value = grid1['RootCode'][i]
        gridId = grid1['RootCode'].index[i]
        try:
            grid2Value = grid2['RootCode'][gridId]
            grid3Value = {str(annotate3['ud_rootCode'][int(gridId)])}
        except KeyError:
            continue
        if grid1['MultiAction'][gridId] == {True} or grid2['MultiAction'][gridId] == {True}:
            continue
        if grid1Value == grid2Value == grid3Value:
            sameRootCode = sameRootCode.append(pd.Series([True], index=[gridId]))
        else:
            sameRootCode = sameRootCode.append(pd.Series([False], index=[gridId]))

    sameRootCodeProp = sameRootCode.value_counts(normalize=True)
    sameRootCodeValue = sameRootCode.value_counts()
    return {'sameRootCodeProp': sameRootCodeProp, 'sameRootCodeValue': sameRootCodeValue}


allRootCodeNoMulti = allRootCodeNoMulti(grid1, grid2, annotate2_3)


# print(allRootCodeNoMulti['sameRootCodeProp'])
# print(allRootCodeNoMulti['sameRootCodeValue'])


# Proportion and absolute value of matching PentaCodes across all annotators no MultiAction(1l)
def allPentaCodeNoMulti(grid1, grid2, annotate3):
    samePentaCode = pd.Series(data=[])
    for i in range(len(grid1['PentaCode'])):
        grid1Value = grid1['PentaCode'][i]
        gridId = grid1['PentaCode'].index[i]
        try:
            grid2Value = grid2['PentaCode'][gridId]
            grid3Value = {str(annotate3['ud_pentaCodes'][int(gridId)])}
        except KeyError:
            continue
        if grid1['MultiAction'][gridId] == {True} or grid2['MultiAction'][gridId] == {True}:
            continue
        if grid1Value == grid2Value == grid3Value:
            samePentaCode = samePentaCode.append(pd.Series([True], index=[gridId]))
        else:
            samePentaCode = samePentaCode.append(pd.Series([False], index=[gridId]))
    samePentaCodeProp = samePentaCode.value_counts(normalize=True)
    samePentaCodeValue = samePentaCode.value_counts()
    return {'samePentaCodeProp': samePentaCodeProp, 'samePentaCodeValue': samePentaCodeValue}


allPentaCodeNoMulti = allPentaCodeNoMulti(grid1, grid2, annotate2_3)


# print(allPentaCodeNoMulti['samePentaCodeProp'])
# print(allPentaCodeNoMulti['samePentaCodeValue'])


# Check how many times MultiAction matches when one annotator says False
def multiActionMatchWhenFalse(grid1, grid2):
    sameMultiAction = pd.Series(data=[])
    for i in range(len(grid1['MultiAction'])):
        grid1Value = grid1['MultiAction'][i]
        gridId = grid1['MultiAction'].index[i]
        try:
            grid2Value = grid2['MultiAction'][gridId]
        except KeyError:
            continue

        if grid1Value == {False} and grid2Value == {False}:
            sameMultiAction = sameMultiAction.append(pd.Series([False], index=[gridId]))
        elif grid1Value == {True} and grid2Value == {False}:
            sameMultiAction = sameMultiAction.append(pd.Series([True], index=[gridId]))
        elif grid1Value == {False} and grid2Value == {True}:
            sameMultiAction = sameMultiAction.append(pd.Series([True], index=[gridId]))
        if grid1Value == {True} and grid2Value == {True}:
            continue

    sameMultiActionProp = sameMultiAction.value_counts(normalize=True)
    sameMultiActionValue = sameMultiAction.value_counts()
    return {'sameMultiActionProp': sameMultiActionProp, 'sameMultiActionValue': sameMultiActionValue}


multiActionMatchWhenFalse = multiActionMatchWhenFalse(grid1, grid2)
# print(multiActionMatchWhenFalse)


# Check how many times both PentaCodes are above -1 when at least one PentaCode is above -1
def pentaCodeMatchWhenBigger(grid1, grid2):
    samePentaCode = pd.Series(data=[])
    for i in range(len(grid1['PentaCode'])):
        grid1Value = grid1['PentaCode'][i]
        gridId = grid1['PentaCode'].index[i]
        try:
            grid2Value = grid2['PentaCode'][gridId]
        except KeyError:
            continue
        if grid1Value in ({'0'}, {'1'}, {'2'}, {'3'}, {'4'}) and grid2Value in ({'0'}, {'1'}, {'2'}, {'3'}, {'4'}):
            samePentaCode = samePentaCode.append(pd.Series([False], index=[gridId]))

        elif grid1Value == {'-1'} and grid2Value in ({'0'}, {'1'}, {'2'}, {'3'}, {'4'}):
            samePentaCode = samePentaCode.append(pd.Series([True], index=[gridId]))

        elif grid1Value in ({'0'}, {'1'}, {'2'}, {'3'}, {'4'}) and grid2Value == {'-1'}:
            samePentaCode = samePentaCode.append(pd.Series([True], index=[gridId]))

        elif grid1Value == {'-1'} and grid2Value == {'-1'}:
            continue

    samePentaCodeProp = samePentaCode.value_counts(normalize=True)
    samePentaCodeValue = samePentaCode.value_counts()
    return {'samePentaCodeProp': samePentaCodeProp, 'samePentaCodeValue': samePentaCodeValue}


pentaCodeMatchWhenBigger = pentaCodeMatchWhenBigger(grid1, grid2)
# print(pentaCodeMatchWhenBigger)


def findUsefulAnnotations(grid1, grid2):
    usefulGrid = pd.DataFrame([])
    for i in range(len(grid1['PentaCode'])):
        grid1Value = grid1['PentaCode'][i]
        gridId = grid1['PentaCode'].index[i]
        try:
            grid2Value = grid2['PentaCode'][gridId]
        except KeyError:
            continue
        if grid1['MultiAction'][gridId] == {True} or grid2['MultiAction'][gridId] == {True}:
            continue
        if grid1Value == grid2Value:
            usefulGrid = usefulGrid.append(grid1.loc[gridId])
        else:
            continue
    return usefulGrid


findUsefulAnnotations = findUsefulAnnotations(grid1, grid2)
# findUsefulAnnotations.to_csv('Group1Useful')