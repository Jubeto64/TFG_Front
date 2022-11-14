from email import message
from subprocess import run, PIPE
import sys
from django.core.paginator import Paginator, EmptyPage, InvalidPage
from django.db.models.aggregates import Count
from django.shortcuts import render,  redirect
from django.core.files.storage import FileSystemStorage
from tables.models import *
from .forms import *
import os
from django.http import JsonResponse
from scripts import *
import json
from django.db.models.functions import TruncMonth
import pika
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="TFG",
    user="postgres",
    password="Untitled#4"
)

def especieList(request):
    desc = request.GET.get('desc')
    sigla = request.GET.get('sigl')
    natureza = request.GET.get('natu')
    if desc or sigla or natureza:
        showall = EspecieModel.objects.filter(
            descricao_especie__icontains=desc, sigla__icontains=sigla, cod_natureza_exame__descricao_natureza__icontains=natureza)
    else:
        showall = EspecieModel.objects.all().order_by('cod_especie_exame')
    return render(request, 'especie/list.html', {"data": showall})


def especieCreate(request, cod_especie_exame=0):
    if request.method == 'GET':
        if cod_especie_exame == 0:
            form = EspecieForm()
        else:
            especie = EspecieModel.objects.get(pk=cod_especie_exame)
            form = EspecieForm(instance=especie)
        return render(request, 'especie/create.html', {'form': form})
    else:
        form = EspecieForm(request.POST)
        if form.is_valid():
            form.save(commit=True)
        return redirect('/especie')


def especieEdit(request, cod_especie_exame=0):
    if request.method == 'GET':
        if cod_especie_exame == 0:
            form = EspecieForm()
        else:
            especie = EspecieModel.objects.get(pk=cod_especie_exame)
            form = EspecieForm(instance=especie)
            form.fields['cod_especie_exame'].widget.attrs['readonly'] = True
        return render(request, 'especie/edit.html', {'form': form})
    else:
        especie = EspecieModel.objects.get(pk=cod_especie_exame)
        form = EspecieForm(request.POST, instance=especie)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/especie')


def naturezaList(request):
    desc = request.GET.get('desc')
    cien = request.GET.get('cien')
    if desc or cien:
        showall = NaturezaModel.objects.filter(
            descricao_natureza__icontains=desc, ciencia__icontains=cien)
    else:
        showall = NaturezaModel.objects.all().order_by('cod_natureza_exame')
    return render(request, 'natureza/list.html', {"data": showall})


def naturezaCreate(request, cod_natureza_exame=0):
    if request.method == 'GET':
        if cod_natureza_exame == 0:
            form = NaturezaForm()
        else:
            natureza = NaturezaModel.objects.get(pk=cod_natureza_exame)
            form = NaturezaForm(instance=natureza)
        return render(request, 'natureza/create.html', {'form': form})
    else:
        form = NaturezaForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/natureza')


def naturezaEdit(request, cod_natureza_exame=0):
    if request.method == 'GET':
        if cod_natureza_exame == 0:
            form = NaturezaForm()
        else:
            natureza = NaturezaModel.objects.get(pk=cod_natureza_exame)
            form = NaturezaForm(instance=natureza)
            form.fields['cod_natureza_exame'].widget.attrs['readonly'] = True
        return render(request, 'natureza/edit.html', {'form': form})
    else:
        natureza = NaturezaModel.objects.get(pk=cod_natureza_exame)
        form = NaturezaForm(request.POST, instance=natureza)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/natureza')


def peritoList(request):
    search = request.GET.get('search')
    if search:
        showall = PeritoModel.objects.filter(nome_perito__icontains=search)
    else:
        showall = PeritoModel.objects.all()
    return render(request, 'perito/list.html', {"data": showall})


def peritoCreate(request, masp=-1):
    if request.method == 'GET':
        if masp == -1:
            form = PeritoForm()
        else:
            perito = PeritoModel.objects.get(pk=masp)
            form = PeritoForm(instance=perito)
        return render(request, 'perito/create.html', {'form': form})
    else:
        form = PeritoForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/perito')


def peritoEdit(request, masp=-1):
    if request.method == 'GET':
        if masp == -1:
            form = PeritoForm()
        else:
            perito = PeritoModel.objects.get(pk=masp)
            form = PeritoForm(instance=perito)
            form.fields['masp'].widget.attrs['readonly'] = True
        return render(request, 'perito/edit.html', {'form': form})
    else:
        perito = PeritoModel.objects.get(pk=masp)
        form = PeritoForm(request.POST, instance=perito)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/perito')


def uniexaList(request):
    search = request.GET.get('search')
    if search:
        showall = UniexaModel.objects.filter(
            comarca_da_unidade__icontains=search.upper())
    else:
        showall = UniexaModel.objects.all()
    return render(request, 'uniexa/list.html', {"data": showall})


def uniexaCreate(request, cod_unidade_exame=''):
    if request.method == 'GET':
        if cod_unidade_exame == '':
            form = UniexaForm()
        else:
            uniexa = UniexaModel.objects.get(pk=cod_unidade_exame)
            form = UniexaForm(instance=uniexa)
        return render(request, 'uniexa/create.html', {'form': form})
    else:
        form = UniexaForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/uniexa')


def uniexaEdit(request, cod_unidade_exame=''):
    if request.method == 'GET':
        if cod_unidade_exame == '':
            form = UniexaForm()
        else:
            uniexa = UniexaModel.objects.get(pk=cod_unidade_exame)
            form = UniexaForm(instance=uniexa)
            form.fields['cod_unidade_exame'].widget.attrs['readonly'] = True
        return render(request, 'uniexa/edit.html', {'form': form})
    else:
        uniexa = UniexaModel.objects.get(pk=cod_unidade_exame)
        form = UniexaForm(request.POST,instance=uniexa)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/uniexa')


def uniresList(request):
    search = request.GET.get('search')
    if search:
        showall = UniresModel.objects.filter(
            municipio__icontains=search.upper())
    else:
        showall = UniresModel.objects.all()
    return render(request, 'unires/list.html', {"data": showall})


def uniresCreate(request, cod_unidade_requisitante=''):
    if request.method == 'GET':
        if cod_unidade_requisitante == '':
            form = UniresForm()
        else:
            unires = UniresModel.objects.get(pk=cod_unidade_requisitante)
            form = UniresForm(instance=unires)
        return render(request, 'unires/create.html', {'form': form})
    else:
        form = UniresForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/unires')


def uniresEdit(request, cod_unidade_requisitante=''):
    if request.method == 'GET':
        if cod_unidade_requisitante == '':
            form = UniresForm()
        else:
            unires = UniresModel.objects.get(pk=cod_unidade_requisitante)
            form = UniresForm(instance=unires)
            form.fields['cod_unidade_requisitante'].widget.attrs['readonly'] = True
        return render(request, 'unires/edit.html', {'form': form})
    else:
        unires = UniresModel.objects.get(pk=cod_unidade_requisitante)
        form = UniresForm(request.POST,instance=unires)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/unires')


def departamentoList(request):
    search = request.GET.get('search')
    if search:
        showall = DepartamentoModel.objects.filter(
            departamento__icontains=search.upper())
    else:
        showall = DepartamentoModel.objects.all()
    return render(request, 'departamento/list.html', {"data": showall})


def departamentoCreate(request, cod_departamento=''):
    if request.method == 'GET':
        if cod_departamento == '':
            form = DepartamentoForm()
        else:
            departamento = DepartamentoModel.objects.get(pk=cod_departamento)
            form = DepartamentoForm(instance=departamento)
        return render(request, 'departamento/create.html', {'form': form})
    else:
        form = DepartamentoForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/departamento')


def departamentoEdit(request, cod_departamento=''):
    if request.method == 'GET':
        if cod_departamento == '':
            form = DepartamentoForm()
        else:
            departamento = DepartamentoModel.objects.get(pk=cod_departamento)
            form = DepartamentoForm(instance=departamento)
            form.fields['cod_departamento'].widget.attrs['readonly'] = True
        return render(request, 'departamento/edit.html', {'form': form})
    else:
        departamento = DepartamentoModel.objects.get(pk=cod_departamento)
        form = DepartamentoForm(request.POST,instance=departamento)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/departamento')


def regionalList(request):
    search = request.GET.get('search')
    if search:
        showall = RegionalModel.objects.filter(
            regional__icontains=search.upper())
    else:
        showall = RegionalModel.objects.all()
    return render(request, 'regional/list.html', {"data": showall})


def regionalCreate(request, cod_regional=''):
    if request.method == 'GET':
        if cod_regional == '':
            form = RegionalForm()
        else:
            regional = RegionalModel.objects.get(pk=cod_regional)
            form = RegionalForm(instance=regional)
        return render(request, 'regional/create.html', {'form': form})
    else:
        form = RegionalForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/regional')


def regionalEdit(request, cod_regional=''):
    if request.method == 'GET':
        if cod_regional == '':
            form = RegionalForm()
        else:
            regional = RegionalModel.objects.get(pk=cod_regional)
            form = RegionalForm(instance=regional)
            form.fields['cod_regional'].widget.attrs['readonly'] = True
        return render(request, 'regional/edit.html', {'form': form})
    else:
        regional = RegionalModel.objects.get(pk=cod_regional)
        form = RegionalForm(request.POST,instance=regional)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/regional')


def municipioList(request):
    search = request.GET.get('search')
    if search:
        showall = MunicipioModel.objects.filter(
            municipio__icontains=search.upper())
    else:
        showall = MunicipioModel.objects.all()
    return render(request, 'municipio/list.html', {"data": showall})


def municipioCreate(request, geocodigo=''):
    if request.method == 'GET':
        if geocodigo == '':
            form = MunicipioForm()
        else:
            municipio = MunicipioModel.objects.get(pk=geocodigo)
            form = MunicipioForm(instance=municipio)
        return render(request, 'municipio/create.html', {'form': form})
    else:
        form = MunicipioForm(request.POST)
        if form.is_valid():
            form.save()
        return redirect('/municipio')


def municipioEdit(request, geocodigo=''):
    if request.method == 'GET':
        if geocodigo == '':
            form = MunicipioForm()
        else:
            municipio = MunicipioModel.objects.get(pk=geocodigo)
            form = MunicipioForm(instance=municipio)
            form.fields['geocodigo'].widget.attrs['readonly'] = True
        return render(request, 'municipio/edit.html', {'form': form})
    else:
        municipio = MunicipioModel.objects.get(pk=geocodigo)
        form = MunicipioForm(request.POST,instance=municipio)
        if form.is_valid():
            form.save(commit=True)
        else:
            print(request.POST)
        return redirect('/municipio')

def upload(request):

    return render((request), 'upload/upload.html')


def run_python_script(request):
    if request.method == 'POST':
        if len(request.FILES) != 0:
            uploaded_file = request.FILES['document']

            fs = FileSystemStorage()
            fs.save(uploaded_file.name, uploaded_file)

            out = run([sys.executable, 'scripts/ETL.py', 'media/' + uploaded_file.name],
                      shell=False, stdout=PIPE, stderr=PIPE)
            print(out.stdout.decode('utf-8'))
            os.remove(path='media/' + uploaded_file.name)
            return render(request, 'upload/upload.html', {"data": out.stdout.decode('utf-8')})
        else:
            return render(request, 'upload/upload.html')


def relatorio_especie(request):
    labels = []
    data = []
    teste = []
    limit = request.GET.get('Qtde')
    dataini = request.GET.get('dataini')
    datafim = request.GET.get('datafim')

    if limit == "Other":
        if request.GET.get('uma') != '':
            espe = int(request.GET.get('uma'))
        else:
            limit ="All"

    if (dataini and datafim):
        dataini = dataini + 'T00:00'
        datafim = datafim + 'T23:59'

    if (dataini and datafim) and limit == 'Ten':
        for especie in EspecieModel.objects.raw('''select l.cod_especie_exame ,n.descricao_especie, count(l.nmr_requisicao) as contagem 
            from laudo l inner join especie_exame n on l.cod_especie_exame = n.cod_especie_exame
            where l.data_requisicao_pericia BETWEEN %s AND %s
            group by n.cod_especie_exame, l.cod_especie_exame order by contagem desc LIMIT 10''',[dataini,datafim]):
            labels.append(especie.cod_especie_exame)
            data.append(especie.contagem)

    elif (dataini and datafim) and limit == 'Other':
        for especie in EspecieModel.objects.raw('''select l.cod_especie_exame ,n.descricao_especie, count(l.nmr_requisicao) as contagem 
            from laudo l inner join especie_exame n on l.cod_especie_exame = n.cod_especie_exame
            where l.data_requisicao_pericia BETWEEN %s AND %s AND n.cod_especie_exame = %s
            group by n.cod_especie_exame, l.cod_especie_exame order by contagem desc''',[dataini,datafim, espe]):
            labels.append(especie.cod_especie_exame)
            data.append(especie.contagem)

    elif not(dataini and datafim) and limit == 'Other':
        for especie in EspecieModel.objects.raw('''select l.cod_especie_exame ,n.descricao_especie, count(l.nmr_requisicao) as contagem 
            from laudo l inner join especie_exame n on l.cod_especie_exame = n.cod_especie_exame
            where n.cod_especie_exame = %s
            group by n.cod_especie_exame, l.cod_especie_exame order by contagem desc''',[espe]):
            labels.append(especie.cod_especie_exame)
            data.append(especie.contagem)

    elif not(dataini and datafim) and limit == 'Ten':
        for especie in EspecieModel.objects.raw('''select l.cod_especie_exame ,n.descricao_especie, count(l.nmr_requisicao) as contagem 
            from laudo l inner join especie_exame n on l.cod_especie_exame = n.cod_especie_exame
            group by n.cod_especie_exame, l.cod_especie_exame order by contagem desc LIMIT 10'''):
            labels.append(especie.cod_especie_exame)
            data.append(especie.contagem)

    elif (dataini and datafim) and limit == 'All':
        for especie in EspecieModel.objects.raw('''select l.cod_especie_exame ,n.descricao_especie, count(l.nmr_requisicao) as contagem
            from laudo l inner join especie_exame n on l.cod_especie_exame = n.cod_especie_exame
            where l.data_requisicao_pericia BETWEEN %s AND %s
            group by n.cod_especie_exame, l.cod_especie_exame order by contagem desc''',[dataini,datafim]):
            labels.append(especie.cod_especie_exame)
            data.append(especie.contagem)

    else:
        
        for especie in EspecieModel.objects.raw('''select l.cod_especie_exame ,n.descricao_especie, count(l.nmr_requisicao) as contagem
        from laudo l inner join especie_exame n on l.cod_especie_exame = n.cod_especie_exame
        group by n.cod_especie_exame, l.cod_especie_exame order by contagem desc'''):
            labels.append(especie.cod_especie_exame)
            teste.append(especie.descricao_especie)
            data.append(especie.contagem)


    context={
        'labels': json.dumps(labels),
        'data': data,
        'teste':teste
    }

    return render(request, 'relatorios/relatorio_especie.html', context)


def relatorio_natureza(request):
    labels = []
    data = []

    limit = 'All'
    limit = request.GET.get('Qtde')
    dataini = request.GET.get('dataini')
    datafim = request.GET.get('datafim')

    if limit == "Other":
        natu = str(request.GET.get('uma'))
        natu = '%' + natu + '%'

    if (dataini and datafim):
        dataini = dataini + 'T00:00'
        datafim = datafim + 'T23:59'
    
    if (dataini and datafim) and limit == 'Ten':
        for natureza in NaturezaModel.objects.raw('''select l.cod_natureza_exame ,n.descricao_natureza, count(l.nmr_requisicao) as contagem 
            from laudo l inner join natureza_exame n on l.cod_natureza_exame = n.cod_natureza_exame
            where l.data_requisicao_pericia BETWEEN %s AND %s
            group by n.cod_natureza_exame, l.cod_natureza_exame order by contagem desc LIMIT 10''',[dataini,datafim]):
            labels.append(natureza.descricao_natureza)
            data.append(natureza.contagem)

    elif (dataini and datafim) and limit == 'Other':
        for natureza in NaturezaModel.objects.raw("""select l.cod_natureza_exame ,n.descricao_natureza, count(l.nmr_requisicao) as contagem 
            from laudo l inner join natureza_exame n on l.cod_natureza_exame = n.cod_natureza_exame
            where (l.data_requisicao_pericia BETWEEN %s AND %s) AND n.descricao_natureza ILIKE  %s 
            group by n.cod_natureza_exame, l.cod_natureza_exame order by contagem desc""",[dataini,datafim,natu]):
            labels.append(natureza.descricao_natureza)
            data.append(natureza.contagem)

    elif not(dataini and datafim) and limit == 'Other':
        for natureza in NaturezaModel.objects.raw("""select l.cod_natureza_exame ,n.descricao_natureza, count(l.nmr_requisicao) as contagem 
            from laudo l inner join natureza_exame n on l.cod_natureza_exame = n.cod_natureza_exame
            where n.descricao_natureza ILIKE  %s 
            group by n.cod_natureza_exame, l.cod_natureza_exame order by contagem desc""",[natu]):
            labels.append(natureza.descricao_natureza)
            data.append(natureza.contagem)

    elif not(dataini and datafim) and limit == 'Ten':
        for natureza in NaturezaModel.objects.raw('''select l.cod_natureza_exame ,n.descricao_natureza, count(l.nmr_requisicao) as contagem 
            from laudo l inner join natureza_exame n on l.cod_natureza_exame = n.cod_natureza_exame
            group by n.cod_natureza_exame, l.cod_natureza_exame order by contagem desc LIMIT 10'''):
            labels.append(natureza.descricao_natureza)
            data.append(natureza.contagem)

    elif (dataini and datafim) and limit == 'All':
        for natureza in NaturezaModel.objects.raw('''select l.cod_natureza_exame ,n.descricao_natureza, count(l.nmr_requisicao) as contagem 
            from laudo l inner join natureza_exame n on l.cod_natureza_exame = n.cod_natureza_exame
            where l.data_requisicao_pericia BETWEEN %s AND %s
            group by n.cod_natureza_exame, l.cod_natureza_exame order by contagem desc''',[dataini,datafim]):
            labels.append(natureza.descricao_natureza)
            data.append(natureza.contagem)

    else:
        for natureza in NaturezaModel.objects.raw('''select l.cod_natureza_exame ,n.descricao_natureza, count(l.nmr_requisicao) as contagem 
        from laudo l inner join natureza_exame n on l.cod_natureza_exame = n.cod_natureza_exame
    group by n.cod_natureza_exame, l.cod_natureza_exame order by contagem desc'''):
            labels.append(natureza.descricao_natureza)
            data.append(natureza.contagem)

    context={
        'labels': json.dumps(labels),
        'data': data,
    }

    return render(request, 'relatorios/relatorio_natureza.html', context)


def relatorio_perito(request):
    labels = []
    data = []

    limit = request.GET.get('Qtde')
    dataini = request.GET.get('dataini')
    datafim = request.GET.get('datafim')

    if limit == "Other":
        if request.GET.get('uma') != '':
            masp = int(request.GET.get('uma'))
        else:
            limit ="All"

    if (dataini and datafim):
        dataini = dataini + 'T00:00'
        datafim = datafim + 'T23:59'

    if (dataini and datafim) and limit == 'Ten':
        for perito in PeritoModel.objects.raw('''select l.masp_perito,pe.masp, count(*) as contagem 
        from laudo l inner join perito_responsavel pe on l.masp_perito = pe.masp
        where l.data_requisicao_pericia BETWEEN %s AND %s
        group by pe.masp, l.masp_perito order by contagem desc LIMIT 10''',[dataini,datafim]):
            labels.append(perito.masp)
            data.append(perito.contagem)

    elif (dataini and datafim) and limit == 'Other':
        for perito in PeritoModel.objects.raw('''select l.masp_perito,pe.masp, count(*) as contagem 
        from laudo l inner join perito_responsavel pe on l.masp_perito = pe.masp
        where l.data_requisicao_pericia BETWEEN %s AND %s AND pe.masp = %s
        group by pe.masp, l.masp_perito order by contagem desc LIMIT 10''',[dataini,datafim, masp]):
            labels.append(perito.masp)
            data.append(perito.contagem)

    elif not(dataini and datafim) and limit == 'Other':
        for perito in PeritoModel.objects.raw('''select l.masp_perito,pe.masp, count(*) as contagem 
        from laudo l inner join perito_responsavel pe on l.masp_perito = pe.masp
        where pe.masp = %s
        group by pe.masp, l.masp_perito order by contagem desc LIMIT 10''',[masp]):
            labels.append(perito.masp)
            data.append(perito.contagem)

    elif not(dataini and datafim) and limit == 'Ten':
        for perito in PeritoModel.objects.raw('''select l.masp_perito,pe.masp, count(*) as contagem 
        from laudo l inner join perito_responsavel pe on l.masp_perito = pe.masp
        group by pe.masp, l.masp_perito order by contagem desc LIMIT 10'''):
            labels.append(perito.masp)
            data.append(perito.contagem)

    elif (dataini and datafim) and limit == 'All':
        for perito in PeritoModel.objects.raw('''select l.masp_perito,pe.masp, count(*) as contagem 
        from laudo l inner join perito_responsavel pe on l.masp_perito = pe.masp
        where l.data_requisicao_pericia BETWEEN %s AND %s
            group by pe.masp, l.masp_perito order by contagem desc''',[dataini,datafim]):
            labels.append(perito.masp)
            data.append(perito.contagem)

    else:
        for perito in PeritoModel.objects.raw('''select l.masp_perito,pe.masp, count(*) as contagem 
        from laudo l inner join perito_responsavel pe on l.masp_perito = pe.masp
            group by pe.masp, l.masp_perito order by contagem desc'''):
            labels.append(perito.masp)
            data.append(perito.contagem)

    context={
        'labels': json.dumps(labels),
        'data': data,
    }

    return render(request, 'relatorios/relatorio_perito.html', context)


def relatorio_unidadex(request):
    labels = []
    data = []

    limit = request.GET.get('Qtde')
    dataini = request.GET.get('dataini')
    datafim = request.GET.get('datafim')

    if limit == "Other":
        unid = str(request.GET.get('uma'))
        unid = '%' + unid + '%'

    if (dataini and datafim):
        dataini = dataini + 'T00:00'
        datafim = datafim + 'T23:59'

    if (dataini and datafim) and limit == 'Ten':
        for unidade in UniexaModel.objects.raw('''select l.cod_unidade_exame ,u.comarca_da_unidade as nome, count(l.nmr_requisicao) as contagem 
        from laudo l inner join unidade_exame u on l.cod_unidade_exame = u.cod_unidade_exame
        where l.data_requisicao_pericia BETWEEN %s AND %s
        group by u.cod_unidade_exame, l.cod_unidade_exame order by contagem desc LIMIT 10''',[dataini,datafim]):
            labels.append(unidade.nome)
            data.append(unidade.contagem)

    elif (dataini and datafim) and limit == 'Other':
        for unidade in UniexaModel.objects.raw('''select l.cod_unidade_exame ,u.comarca_da_unidade as nome, count(l.nmr_requisicao) as contagem 
        from laudo l inner join unidade_exame u on l.cod_unidade_exame = u.cod_unidade_exame
        where l.data_requisicao_pericia BETWEEN %s AND %s AND u.comarca_da_unidade ILIKE %s
        group by u.cod_unidade_exame, l.cod_unidade_exame order by contagem desc''',[dataini,datafim, unid]):
            labels.append(unidade.nome)
            data.append(unidade.contagem)

    elif not(dataini and datafim) and limit == 'Other':
        for unidade in UniexaModel.objects.raw('''select l.cod_unidade_exame ,u.comarca_da_unidade as nome, count(l.nmr_requisicao) as contagem 
        from laudo l inner join unidade_exame u on l.cod_unidade_exame = u.cod_unidade_exame
        where u.comarca_da_unidade ILIKE %s
        group by u.cod_unidade_exame, l.cod_unidade_exame order by contagem desc''',[unid]):
            labels.append(unidade.nome)
            data.append(unidade.contagem)

    elif not(dataini and datafim) and limit == 'Ten':
        for unidade in UniexaModel.objects.raw('''select l.cod_unidade_exame ,u.comarca_da_unidade as nome, count(l.nmr_requisicao) as contagem 
        from laudo l inner join unidade_exame u on l.cod_unidade_exame = u.cod_unidade_exame
        group by u.cod_unidade_exame, l.cod_unidade_exame order by contagem desc LIMIT 10'''):
            labels.append(unidade.nome)
            data.append(unidade.contagem)
    
    elif (dataini and datafim) and limit == 'All':
        for unidade in UniexaModel.objects.raw('''select l.cod_unidade_exame ,u.comarca_da_unidade as nome, count(l.nmr_requisicao) as contagem 
        from laudo l inner join unidade_exame u on l.cod_unidade_exame = u.cod_unidade_exame
        where l.data_requisicao_pericia BETWEEN %s AND %s
        group by u.cod_unidade_exame, l.cod_unidade_exame order by contagem desc''',[dataini,datafim]):
            labels.append(unidade.nome)
            data.append(unidade.contagem)

    else:
        for unidade in UniexaModel.objects.raw('''select l.cod_unidade_exame ,u.comarca_da_unidade as nome, count(l.nmr_requisicao) as contagem 
        from laudo l inner join unidade_exame u on l.cod_unidade_exame = u.cod_unidade_exame
        group by u.cod_unidade_exame, l.cod_unidade_exame order by contagem desc'''):
            labels.append(unidade.nome)
            data.append(unidade.contagem)

    context={
        'labels': json.dumps(labels),
        'data': data,
    }

    return render(request, 'relatorios/relatorio_unidadex.html', context)


def relatorio_unidader(request):
    labels = []
    data = []

    limit = request.GET.get('Qtde')
    dataini = request.GET.get('dataini')
    datafim = request.GET.get('datafim')
    depunires = request.GET.get('depunires')
    regunires = request.GET.get('regunires')
    mununires = request.GET.get('mununires')

    if depunires: depunires = '%' + depunires + '%'
    else: depunires = '%'

    if regunires: regunires = '%' + regunires + '%'
    else: regunires = '%'

    if mununires: mununires = '%' + mununires + '%'
    else: mununires = '%'

    if limit == 'Ten': limit = ' LIMIT 10'
    else: limit = ''

    select_from = '''
        select l.cod_unidade_requisitante, d.departamento, r.regional, m.municipio, count(l.nmr_requisicao) as contagem 
        from    laudo l 
                inner join unidade_requisitante u 
                    on l.cod_unidade_requisitante = u.cod_unidade_requisitante
                inner join municipio m
                    on u.geocodigo = m.geocodigo
                inner join regional r
                    on m.cod_regional = r.cod_regional
                inner join departamento d
                    on r.cod_departamento = d.cod_departamento
    '''
    group_by = 'group by l.cod_unidade_requisitante, d.departamento, r.regional, m.municipio order by contagem desc'

    if (dataini and datafim):
        dataini = dataini + 'T00:00'
        datafim = datafim + 'T23:59'

        for unidade in UniresModel.objects.raw(
            select_from + 
            ''' where   l.data_requisicao_pericia BETWEEN %s AND %s
                        AND d.departamento ILIKE %s
                        AND r.regional ILIKE %s
                        AND m.municipio ILIKE %s''' 
            + group_by + limit,[dataini, datafim, depunires, regunires, mununires]
        ):
            labels.append(str(unidade.cod_unidade_requisitante + ' - ' + unidade.departamento + ' - ' + unidade.regional + ' - ' + unidade.municipio))
            data.append(unidade.contagem)
        
    else:
        for unidade in UniresModel.objects.raw(select_from + 
            ''' where   d.departamento ILIKE %s
                        AND r.regional ILIKE %s
                        AND m.municipio ILIKE %s'''
            + group_by + limit,[depunires, regunires, mununires]
        ):
            labels.append(str(unidade.cod_unidade_requisitante + ' - ' + unidade.departamento + ' - ' + unidade.regional + ' - ' + unidade.municipio))
            data.append(unidade.contagem)

    context={
        'labels': json.dumps(labels),
        'data': data,
    }

    return render(request, 'relatorios/relatorio_unidader.html', context)


def adhoc(request):
    natu = request.GET.get('descricao_natureza')
    codnatu = request.GET.get('cod_natureza_exame')
    espe = request.GET.get('descricao_especie')
    codespe = request.GET.get('cod_especie_exame')
    classespe = request.GET.get('sigla')
    masp = request.GET.get('masp')
    depunires = request.GET.get('departamento')
    regunires = request.GET.get('regional')
    mununires = request.GET.get('municipio')
    uniex = request.GET.get('comarca_da_unidade')
    tpres = request.GET.get('tipo_requisicao')
    tipodata = request.GET.get('tipodata')
    dataini = ''
    datafim = ''
    if tipodata == "requisicao":
        dataini = request.GET.get('datainireq')
        datafim = request.GET.get('datafimreq')
    elif tipodata == "expedicao":
        dataini = request.GET.get('datainiexp')
        datafim = request.GET.get('datafimexp')

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)

    consulta = request.GET.copy()
    filtros = consulta.copy()
    campos = []

    filtros_consulta = []

    if(consulta != {}):
        campos = request.GET.getlist('campos')
        if len(campos) == 19:
            campos = ['*']
        consulta['campos'] = campos

        filtros.pop('campos')
        filtros.pop('page')
        filtros.pop('tipodata')
        for key, value in filtros.items():
            if(value != ''):
                if(key == 'datainireq'):
                    filtros_consulta.append('data_requisicao_pericia >= \'' + value + '\'')
                
                elif(key == 'datafimreq'):
                    filtros_consulta.append('data_requisicao_pericia <= \'' + value + '\'')

                elif(key == 'datainiexp'):
                    filtros_consulta.append('data_expedicao_laudo >= \'' + value + '\'')
                
                elif(key == 'datafimexp'):
                    filtros_consulta.append('data_expedicao_laudo <= \'' + value + '\'')

                elif(key == 'cod_natureza_exame'):
                    filtros_consulta.append('laudo.cod_natureza_exame = ' + value)

                elif(key == 'cod_especie_exame'):
                    filtros_consulta.append('laudo.cod_especie_exame = ' + value)

                elif(key == 'municipio'):
                    filtros_consulta.append('municipio.municipio like \'%' + value + '%\'')

                else:
                    if(value.isnumeric()):
                        filtros_consulta.append(key + ' = ' + value)
                    else:
                        filtros_consulta.append(key + ' like \'%' + value +'%\'')


    campos = list(map(lambda x: x.replace('municipio', 'municipio.municipio'), campos))

    consulta_sql = 'select '
    
    consulta_sql += ','.join(campos)

    consulta_sql += '''\nfrom laudo
	 left join unidade_requisitante unires
	 	on laudo.cod_unidade_requisitante = unires.cod_unidade_requisitante
	 left join unidade_exame uniexa
	 	on laudo.cod_unidade_exame = uniexa.cod_unidade_exame
	 left join perito_responsavel perito
	 	on laudo.masp_perito = perito.masp
	 left join natureza_exame natureza
	 	on laudo.cod_natureza_exame = natureza.cod_natureza_exame
	 left join especie_exame especie
	 	on especie.cod_especie_exame = laudo.cod_especie_exame
	 left join municipio
	 	on unires.geocodigo = municipio.geocodigo
	 left join regional
	 	on municipio.cod_regional = regional.cod_regional
	 left join departamento
	 	on regional.cod_departamento = departamento.cod_departamento'''

    if(len(filtros_consulta) > 0):
        consulta_sql += '\nWhere ' + ' and '.join(filtros_consulta)

    #print(consulta_sql)

    cur = conn.cursor()
    cur.execute(consulta_sql)
    print(cur.fetchall())

    cur.close()


    

    message = json.dumps(consulta, ensure_ascii=False).encode('utf8')

    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        )
    )

    connection.close()


    if not(dataini and datafim):
        if natu or codnatu or espe or codespe or classespe or masp or depunires or regunires or mununires or uniex or tpres:
            showall = LaudoModel.objects.filter(
                cod_natureza_exame__descricao_natureza__icontains=natu, cod_natureza_exame__cod_natureza_exame__icontains=codnatu, cod_especie_exame__descricao_especie__icontains=espe, cod_especie_exame__sigla__icontains=classespe, cod_especie_exame__cod_especie_exame__icontains=codespe, masp_perito__icontains=masp, cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento__icontains=depunires, cod_unidade_requisitante__geocodigo__cod_regional__regional__icontains=regunires, cod_unidade_requisitante__geocodigo__municipio__icontains=mununires, cod_unidade_exame__comarca_da_unidade__icontains=uniex, tipo_requisicao__icontains=tpres
            ).order_by('nmr_requisicao')
        else:
            showall = LaudoModel.objects.all().order_by('nmr_requisicao')[:500]
    else:
        if tipodata == "requisicao":
            if natu or codnatu or espe or codespe or classespe or masp or depunires or regunires or mununires or uniex or tpres or (dataini and datafim):
                showall = LaudoModel.objects.filter(
                    cod_natureza_exame__descricao_natureza__icontains=natu, cod_natureza_exame__cod_natureza_exame__icontains=codnatu, cod_especie_exame__descricao_especie__icontains=espe, cod_especie_exame__sigla__icontains=classespe, cod_especie_exame__cod_especie_exame__icontains=codespe, masp_perito__icontains=masp, cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento__icontains=depunires, cod_unidade_requisitante__geocodigo__cod_regional__regional__icontains=regunires, cod_unidade_requisitante__geocodigo__municipio__icontains=mununires, cod_unidade_exame__comarca_da_unidade__icontains=uniex, tipo_requisicao__icontains=tpres, data_requisicao_pericia__range=[dataini, datafim]
                ).order_by('nmr_requisicao')
        elif tipodata == "expedicao":
            if natu or codnatu or espe or codespe or classespe or masp or depunires or regunires or mununires or uniex or tpres or (dataini and datafim):
                showall = LaudoModel.objects.filter(
                    cod_natureza_exame__descricao_natureza__icontains=natu, cod_natureza_exame__cod_natureza_exame__icontains=codnatu, cod_especie_exame__descricao_especie__icontains=espe, cod_especie_exame__sigla__icontains=classespe, cod_especie_exame__cod_especie_exame__icontains=codespe, masp_perito__icontains=masp, cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento__icontains=depunires, cod_unidade_requisitante__geocodigo__cod_regional__regional__icontains=regunires, cod_unidade_requisitante__geocodigo__municipio__icontains=mununires, cod_unidade_exame__comarca_da_unidade__icontains=uniex, tipo_requisicao__icontains=tpres, data_expedicao_laudo__range=[dataini, datafim]
                ).order_by('nmr_requisicao')
    
    p = Paginator(showall,2000)
    page = request.GET.get('page', 1)

    try:
        data = p.get_page(page)
        #print(data.paginator.num_pages)
        #print(data.number)
    except (EmptyPage, InvalidPage):
        data = p.page(p.num_pages)
    return render(request, 'relatorios/relatorio_adhoc.html', {'data':data, 'campos':campos})


def dash(request):
    natureza = NaturezaModel.objects.all().order_by('descricao_natureza')
    return render(request, 'dashboard/dash.html',{"data": natureza})


def home(request):
    return render(request, 'home.html')


def dadosMapa(request):
    natu = request.GET.get('natureza')
    inicio = request.GET.get('inicio')
    fim = request.GET.get('fim')

    if not natu and not inicio:
        data = LaudoModel.objects.values('cod_unidade_requisitante__geocodigo', 'cod_unidade_requisitante__geocodigo__cod_regional__regional', 'cod_unidade_requisitante__geocodigo__cod_regional', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento').annotate(value=Count('nmr_requisicao')).order_by('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__regional')
    elif natu and not(inicio and fim):
        data = LaudoModel.objects.values('cod_unidade_requisitante__geocodigo', 'cod_unidade_requisitante__geocodigo__cod_regional__regional', 'cod_unidade_requisitante__geocodigo__cod_regional', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento').annotate(value=Count('nmr_requisicao')).order_by('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__regional').filter(cod_natureza_exame=natu)
    elif not natu and (inicio and fim):
        data = LaudoModel.objects.values('cod_unidade_requisitante__geocodigo', 'cod_unidade_requisitante__geocodigo__cod_regional__regional', 'cod_unidade_requisitante__geocodigo__cod_regional', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento').annotate(value=Count('nmr_requisicao')).order_by('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__regional').filter(data_requisicao_pericia__range=[inicio, fim])
    elif natu and (inicio and fim):
        data = LaudoModel.objects.values('cod_unidade_requisitante__geocodigo', 'cod_unidade_requisitante__geocodigo__cod_regional__regional', 'cod_unidade_requisitante__geocodigo__cod_regional', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento').annotate(value=Count('nmr_requisicao')).order_by('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__departamento', 'cod_unidade_requisitante__geocodigo__cod_regional__regional').filter(cod_natureza_exame=natu, data_requisicao_pericia__range=[inicio, fim])

    return JsonResponse(list(data),safe=False)


def dadosLinha(request,geocod):
    natu = request.GET.get('natureza')
    inicio = request.GET.get('inicio')
    fim = request.GET.get('fim')
    
    if not natu and not inicio:
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__geocodigo__icontains=geocod)
    elif natu and not(inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__geocodigo__icontains=geocod).filter(cod_natureza_exame=natu)
    elif not natu and (inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__geocodigo__icontains=geocod).filter(data_requisicao_pericia__range=[inicio, fim])
    elif natu and (inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__geocodigo__icontains=geocod).filter(data_requisicao_pericia__range=[inicio, fim], cod_natureza_exame=natu)
    return JsonResponse(list(data),safe=False)


def dadosLinhaRegional(request,regional):
    natu = request.GET.get('natureza')
    inicio = request.GET.get('inicio')
    fim = request.GET.get('fim')
    
    if not natu and not inicio:
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_regional__icontains=regional)
    elif natu and not(inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_regional__icontains=regional).filter(cod_natureza_exame=natu)
    elif not natu and (inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_regional__icontains=regional).filter(data_requisicao_pericia__range=[inicio, fim])
    elif natu and (inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_regional__icontains=regional).filter(data_requisicao_pericia__range=[inicio, fim], cod_natureza_exame=natu)
    return JsonResponse(list(data),safe=False)


def dadosLinhaDepartamento(request,departamento):
    natu = request.GET.get('natureza')
    inicio = request.GET.get('inicio')
    fim = request.GET.get('fim')
    
    if not natu and not inicio:
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__cod_departamento__icontains=departamento)
    elif natu and not(inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__cod_departamento__icontains=departamento).filter(cod_natureza_exame=natu)
    elif not natu and (inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__cod_departamento__icontains=departamento).filter(data_requisicao_pericia__range=[inicio, fim])
    elif natu and (inicio and fim):
        data = LaudoModel.objects.annotate(mes_registro=TruncMonth('data_requisicao_pericia')).values('cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento','mes_registro').annotate(laudo_nmr=Count('nmr_requisicao')).order_by('mes_registro').filter(cod_unidade_requisitante__geocodigo__cod_regional__cod_departamento__cod_departamento__icontains=departamento).filter(data_requisicao_pericia__range=[inicio, fim], cod_natureza_exame=natu)
    return JsonResponse(list(data),safe=False)